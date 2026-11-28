mod decode_sizes;
mod playout_buffer;
mod ssrc_state;

use self::{decode_sizes::*, playout_buffer::*, ssrc_state::*};

use super::message::*;
use crate::driver::{CryptoMode, DecodeMode};
use crate::{
    constants::*,
    driver::crypto::Cipher,
    events::{context_data::VoiceTick, internal_data::*, CoreContext},
    Config,
};
use bytes::BytesMut;
use discortp::{
    demux::{self, DemuxedMut},
    rtp::RtpPacket,
};
use discortp::{MutablePacket, Packet};
use flume::Receiver;
use std::sync::{
    atomic::{AtomicU16, Ordering},
    RwLock,
};
use std::{
    collections::{HashMap, HashSet},
    num::Wrapping,
    sync::Arc,
    time::Duration,
};
use tokio::{net::UdpSocket, select, time::Instant};
use tracing::{error, instrument, trace, warn};

type RtpSequence = Wrapping<u16>;
type RtpTimestamp = Wrapping<u32>;
type RtpSsrc = u32;

struct UdpRx {
    cipher: Cipher,
    crypto_mode: CryptoMode,
    decoder_map: HashMap<RtpSsrc, SsrcState>,
    config: Config,
    rx: Receiver<UdpRxMessage>,
    ssrc_signalling: Arc<SsrcTracker>,
    udp_socket: UdpSocket,
    dave_session: Arc<RwLock<Option<davey::DaveSession>>>,
    dave_protocol_version: Arc<AtomicU16>,
}

impl UdpRx {
    #[instrument(skip(self))]
    async fn run(&mut self, interconnect: &mut Interconnect) {
        let mut cleanup_time = Instant::now();
        let mut playout_time = Instant::now() + TIMESTEP_LENGTH;
        let mut byte_dest: Option<BytesMut> = None;

        loop {
            if byte_dest.is_none() {
                byte_dest = Some(BytesMut::zeroed(VOICE_PACKET_MAX));
            }

            select! {
                Ok((len, _addr)) = self.udp_socket.recv_from(byte_dest.as_mut().unwrap()) => {
                    let mut pkt = byte_dest.take().unwrap();
                    pkt.truncate(len);

                    self.process_udp_message(interconnect, pkt).await;
                },
                msg = self.rx.recv_async() => {
                    match msg {
                        Ok(UdpRxMessage::ReplaceInterconnect(i)) => {
                            *interconnect = i;
                        },
                        Ok(UdpRxMessage::SetConfig(new_config)) => {
                            if let DecodeMode::Decode(old_config) = &mut self.config.decode_mode {
                                if *old_config != new_config {
                                    *old_config = new_config;
                                    self.decoder_map.values_mut().for_each(|v| v.reconfigure_decoder(new_config));
                                }
                            }
                        },
                        Err(flume::RecvError::Disconnected) => break,
                    }
                },
                () = tokio::time::sleep_until(playout_time) => {
                    let mut tick = VoiceTick {
                        speaking: HashMap::new(),
                        silent: HashSet::new(),
                    };

                    for (ssrc, state) in &mut self.decoder_map {
                        match state.get_voice_tick(&self.config) {
                            Ok(Some(data)) => {
                                tick.speaking.insert(*ssrc, data);
                            },
                            Ok(None) => {
                                if !state.disconnected {
                                    tick.silent.insert(*ssrc);
                                }
                            },
                            Err(e) => {
                                warn!("Decode error for SSRC {ssrc}: {e:?}");
                                tick.silent.insert(*ssrc);
                            },
                        }
                    }

                    playout_time += TIMESTEP_LENGTH;

                    drop(interconnect.events.send(EventMessage::FireCoreEvent(CoreContext::VoiceTick(tick))));
                },
                () = tokio::time::sleep_until(cleanup_time) => {
                    // periodic cleanup.
                    let now = Instant::now();

                    // check ssrc map to see if the WS task has informed us of any disconnects.
                    loop {
                        // This is structured in an odd way to prevent deadlocks.
                        // while-let seemed to keep the dashmap iter() alive for block scope, rather than
                        // just the initialiser.
                        let id = {
                            if let Some(id) = self.ssrc_signalling.disconnected_users.iter().next().map(|v| *v.key()) {
                                id
                            } else {
                                break;
                            }
                        };

                        _ = self.ssrc_signalling.disconnected_users.remove(&id);
                        if let Some((_, ssrc)) = self.ssrc_signalling.user_ssrc_map.remove(&id) {
                            let _ = self.ssrc_signalling.ssrc_user_map.remove(&ssrc);

                            if let Some(state) = self.decoder_map.get_mut(&ssrc) {
                                // don't cleanup immediately: leave for later cycle
                                // this is key with reorder/jitter buffers where we may
                                // still need to decode post disconnect for ~0.2s.
                                state.prune_time = now + Duration::from_secs(1);
                                state.disconnected = true;
                            }
                        }
                    }

                    // now remove all dead ssrcs.
                    self.decoder_map.retain(|_, v| v.prune_time > now);

                    cleanup_time = now + Duration::from_secs(5);
                },
            }
        }
    }

    async fn process_udp_message(&mut self, interconnect: &Interconnect, mut packet: BytesMut) {
        // NOTE: errors here (and in general for UDP) are not fatal to the connection.
        // Panics should be avoided due to adversarial nature of rx'd packets,
        // but correct handling should not prompt a reconnect.
        //
        // For simplicity, if the event task fails then we nominate the mixing thread
        // to rebuild their context etc. (hence, the `let _ =` statements.), as it will
        // try to make contact every 20ms.
        let crypto_mode = self.crypto_mode;

        match demux::demux_mut(packet.as_mut()) {
            DemuxedMut::Rtp(mut rtp) => {
                if !rtp_valid(&rtp.to_immutable()) {
                    error!("Illegal RTP message received.");
                    return;
                }

                let mut packet_data = if self.config.decode_mode.should_decrypt() {
                    let out = self.cipher.decrypt_rtp_in_place(&mut rtp).map(|(s, t)| {
                        if rtp.get_padding() != 0 {
                            let payload = rtp.payload();
                            let payload_length = payload.len();
                            let padding_count = payload[payload_length - t - 1] as usize;

                            (s, t + padding_count, true)
                        } else {
                            (s, t, true)
                        }
                    });

                    if let Err(ref e) = out {
                        warn!("RTP decryption failed: {:?}", e);
                    }

                    out.ok()
                } else {
                    None
                };

                if let Some((rtp_body_start, rtp_body_tail, true)) = packet_data {
                    let raw = rtp.packet();
                    let pt = raw[1] & 0x7f;
                    if pt != crate::video::OPUS_PT {
                        let marker = raw[1] & 0x80 != 0;
                        let seq = u16::from_be_bytes([raw[2], raw[3]]);
                        let timestamp = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);
                        let ssrc = rtp.get_ssrc();
                        let payload = rtp.payload();
                        if crate::video::take_keyframe_request(ssrc) {
                            let media = ssrc;
                            let ours = crate::video::our_ssrc();
                            let mut header = [0u8; 8];
                            header[0] = 0x81; // V=2, FMT=1 (Picture Loss Indication)
                            header[1] = 206; // PSFB
                            header[2..4].copy_from_slice(&2u16.to_be_bytes());
                            header[4..8].copy_from_slice(&ours.to_be_bytes());
                            let counter = rand::random::<u32>();
                            if let Ok(pkt) = self.cipher.seal_rtcp(&header, &media.to_be_bytes(), counter) {
                                let _ = self.udp_socket.try_send(&pkt);
                                tracing::info!("[rustcord] keyframe request sent for SSRC {media}");
                            }
                        }
                        if payload.len() >= rtp_body_start + rtp_body_tail {
                            let body = payload[rtp_body_start..payload.len() - rtp_body_tail].to_vec();
                            crate::video::deliver(crate::video::RawVideo {
                                ssrc, seq, timestamp, marker, payload_type: pt, body,
                                dave: self.dave_session.clone(),
                            });
                        }
                        return;
                    }
                }

                if let Some((rtp_body_start, rtp_body_tail, decrypted)) = packet_data {
                    let ssrc = rtp.get_ssrc();
                    let payload = rtp.payload_mut();
                    let payload_length = payload.len();
                    let body = &mut payload[rtp_body_start..payload_length - rtp_body_tail];
                    let body_length = body.len();

                    // If the packet is transport-decrypted, DAVE is enabled, and the packet
                    // looks encrypted: https://daveprotocol.com/#protocol-frame-check
                    // The packet must be at least 11 bytes consisting of:
                    // - 8 byte truncated AES128-GCM authentication tag
                    // - 1 byte protocol supplemental data size
                    // - 2 byte magic marker
                    if decrypted
                        && self.dave_protocol_version.load(Ordering::Relaxed) != 0
                        && body_length >= 11
                        && body[body_length - DAVE_MAGIC_MARKER.len()..body_length]
                            == DAVE_MAGIC_MARKER
                    {
                        // Silently drop encrypted packets if it's not possible to decrypt them for
                        // one reason or another; otherwise there'd be error logs for trying to decode
                        // Opus
                        let Some(user_id) = self.ssrc_signalling.ssrc_user_map.get(&ssrc) else {
                            return;
                        };
                        let Some(ref mut dave_session) = *self.dave_session.write().unwrap() else {
                            return;
                        };

                        if !dave_session.is_ready() {
                            return;
                        }

                        let result = dave_session.decrypt(user_id.0, davey::MediaType::AUDIO, body);

                        match result {
                            Ok(decrypted_body) => {
                                packet_data = Some((
                                    rtp_body_start,
                                    rtp_body_tail + (body.len() - decrypted_body.len()),
                                    decrypted,
                                ));
                                body[..decrypted_body.len()].copy_from_slice(&decrypted_body);
                            },
                            Err(davey::errors::DecryptError::NoDecryptorForUser)
                            | Err(davey::errors::DecryptError::DecryptionFailed(
                                davey::errors::DecryptorDecryptError::NoValidCryptorFound {
                                    ..
                                },
                            )) => {
                                // Silently drop encrypted packets for users whose ratchets are not configured yet.
                                return;
                            },
                            Err(e) => {
                                error!(error = ?e, "DAVE decryption failed");
                                return;
                            },
                        }
                    }
                }

                let rtp = rtp.to_immutable();
                let (rtp_body_start, rtp_body_tail, decrypted) = packet_data.unwrap_or_else(|| {
                    (
                        crypto_mode.payload_prefix_len(),
                        crypto_mode.payload_suffix_len(),
                        false,
                    )
                });

                let entry = self
                    .decoder_map
                    .entry(rtp.get_ssrc())
                    .or_insert_with(|| SsrcState::new(&rtp, crypto_mode, &self.config));

                // Only do this on RTP, rather than RTCP -- this pins decoder state liveness
                // to *speech* rather than just presence.
                entry.refresh_timer(self.config.decode_state_timeout.into());

                let store_pkt = StoredPacket {
                    packet: packet.freeze(),
                    decrypted,
                };
                let packet = store_pkt.packet.clone();
                entry.store_packet(store_pkt, &self.config);

                drop(interconnect.events.send(EventMessage::FireCoreEvent(
                    CoreContext::RtpPacket(InternalRtpPacket {
                        packet,
                        payload_offset: rtp_body_start,
                        payload_end_pad: rtp_body_tail,
                    }),
                )));
            },
            DemuxedMut::Rtcp(mut rtcp) => {
                // Rustcord: a viewer asked for a full picture (PLI) for our outgoing stream.
                {
                    let raw = rtcp.packet();
                    if raw.len() >= 2 && raw[1] == 206 && (raw[0] & 0x1f) == 1 && (crate::video::is_sending() || crate::video::camera_ready()) {
                        crate::video::keyframe_wanted();
                    }
                }
                let packet_data = if self.config.decode_mode.should_decrypt() {
                    let out = self.cipher.decrypt_rtcp_in_place(&mut rtcp);

                    if let Err(ref e) = out {
                        warn!("RTCP decryption failed: {:?}", e);
                    }

                    out.ok()
                } else {
                    None
                };

                let (start, tail) = packet_data.unwrap_or_else(|| {
                    (
                        crypto_mode.payload_prefix_len(),
                        crypto_mode.payload_suffix_len(),
                    )
                });

                drop(interconnect.events.send(EventMessage::FireCoreEvent(
                    CoreContext::RtcpPacket(InternalRtcpPacket {
                        packet: packet.freeze(),
                        payload_offset: start,
                        payload_end_pad: tail,
                    }),
                )));
            },
            DemuxedMut::FailedParse(t) => {
                warn!("Failed to parse message of type {:?}.", t);
            },
            DemuxedMut::TooSmall => {
                warn!("Illegal UDP packet from voice server.");
            },
        }
    }
}

#[instrument(skip(interconnect, rx, cipher))]
pub(crate) async fn runner(
    mut interconnect: Interconnect,
    rx: Receiver<UdpRxMessage>,
    cipher: Cipher,
    crypto_mode: CryptoMode,
    config: Config,
    udp_socket: UdpSocket,
    ssrc_signalling: Arc<SsrcTracker>,
    dave_session: Arc<RwLock<Option<davey::DaveSession>>>,
    dave_protocol_version: Arc<AtomicU16>,
) {
    trace!("UDP receive handle started.");

    let mut state = UdpRx {
        cipher,
        crypto_mode,
        decoder_map: HashMap::new(),
        config,
        rx,
        ssrc_signalling,
        udp_socket,
        dave_session,
        dave_protocol_version,
    };

    state.run(&mut interconnect).await;

    trace!("UDP receive handle stopped.");
}

#[inline]
fn rtp_valid(packet: &RtpPacket<'_>) -> bool {
    // Rustcord: video packets (other payload types) are valid too; they are routed to crate::video.
    let _ = RTP_PROFILE_TYPE;
    packet.get_version() == RTP_VERSION
}

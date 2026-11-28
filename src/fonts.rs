// Font fallback on demand: when a name or message uses a character none of the loaded fonts
// can draw (it would show as a box), search the installed Windows fonts in the background,
// smallest first, and load one that has it. Only fonts that are actually needed get loaded.

use ab_glyph::{Font, FontRef};
use eframe::egui;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

static DEFS: Mutex<Option<egui::FontDefinitions>> = Mutex::new(None);
static FACES: Mutex<Vec<FontRef<'static>>> = Mutex::new(Vec::new());
static SEEN: Mutex<Option<HashSet<char>>> = Mutex::new(None);
static PENDING: Mutex<Vec<char>> = Mutex::new(Vec::new());
static TRIED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
static SEARCHING: AtomicBool = AtomicBool::new(false);

/// Font bytes kept for the whole run (egui needs them forever anyway); no copies are made.
pub fn leak(bytes: Vec<u8>) -> &'static [u8] {
    Box::leak(bytes.into_boxed_slice())
}

/// Remember a loaded font so we know which characters are already covered.
pub fn register(bytes: &'static [u8], index: u32) {
    if let Ok(f) = FontRef::try_from_slice_and_index(bytes, index) {
        FACES.lock().unwrap().push(f);
    }
}

pub fn install(ctx: &egui::Context, defs: egui::FontDefinitions) {
    ctx.set_fonts(defs.clone());
    *DEFS.lock().unwrap() = Some(defs);
}

fn skip(c: char) -> bool {
    c.is_ascii() || c.is_control() || matches!(c as u32, 0xE000..=0xF8FF | 0xF0000..=0x10FFFF) || invisible(c)
}

/// Joiners / variation selectors / tags: they change how emoji look in Discord but have no
/// glyph of their own, so they would be drawn as boxes here.
pub fn invisible(c: char) -> bool {
    matches!(c as u32, 0x200B..=0x200F | 0x2060..=0x2064 | 0xFE00..=0xFE0F | 0xFEFF | 0xE0000..=0xE007F | 0x034F | 0x180E)
}

fn covered(c: char) -> bool {
    FACES.lock().unwrap().iter().any(|f| f.glyph_id(c).0 != 0)
}

/// Look at text that is about to be shown; fetch fonts for anything that can't be drawn.
pub fn check(ctx: &egui::Context, text: &str) {
    let mut fresh = Vec::new();
    {
        let mut seen = SEEN.lock().unwrap();
        let seen = seen.get_or_insert_with(HashSet::new);
        for c in text.chars() {
            if !skip(c) && seen.insert(c) {
                fresh.push(c);
            }
        }
    }
    if fresh.is_empty() && PENDING.lock().unwrap().is_empty() {
        return;
    }
    {
        let mut pending = PENDING.lock().unwrap();
        pending.extend(fresh.into_iter().filter(|c| !covered(*c)));
        if pending.is_empty() || SEARCHING.swap(true, Ordering::SeqCst) {
            return;
        }
    }
    let ctx = ctx.clone();
    std::thread::spawn(move || {
        search(&ctx);
        SEARCHING.store(false, Ordering::SeqCst);
    });
}

fn search(ctx: &egui::Context) {
    let mut want: Vec<char> = std::mem::take(&mut *PENDING.lock().unwrap());
    want.retain(|c| !covered(*c));
    if want.is_empty() {
        return;
    }
    let dir = std::env::var("WINDIR").unwrap_or_else(|_| "C:\\Windows".into()) + "\\Fonts";
    let mut files: Vec<(u64, std::path::PathBuf)> = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| {
                    let p = e.path();
                    let ext = p.extension()?.to_str()?.to_ascii_lowercase();
                    let size = e.metadata().ok()?.len();
                    (matches!(ext.as_str(), "ttf" | "otf" | "ttc") && size < 40_000_000).then_some((size, p))
                })
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    let mut added = Vec::new();
    for (_, path) in files {
        if want.is_empty() {
            break;
        }
        let key = path.to_string_lossy().to_lowercase();
        if TRIED.lock().unwrap().get_or_insert_with(HashSet::new).contains(&key) {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else { continue };
        let Ok(face) = FontRef::try_from_slice_and_index(&bytes, 0) else { continue };
        if !want.iter().any(|c| face.glyph_id(*c).0 != 0) {
            continue;
        }
        want.retain(|c| face.glyph_id(*c).0 == 0);
        drop(face);
        TRIED.lock().unwrap().get_or_insert_with(HashSet::new).insert(key.clone());
        let stat = leak(bytes);
        register(stat, 0);
        added.push((key, stat));
    }
    if !want.is_empty() {
        tracing::info!("fonts: no installed font has {:?}", want.iter().map(|c| format!("U+{:04X}", *c as u32)).collect::<Vec<_>>());
    }
    if added.is_empty() {
        return;
    }
    let mut guard = DEFS.lock().unwrap();
    let Some(defs) = guard.as_mut() else { return };
    for (key, stat) in added {
        tracing::info!("fonts: loaded {key} for missing characters");
        let name = format!("fallback:{key}");
        defs.font_data.insert(name.clone(), egui::FontData::from_static(stat));
        for list in defs.families.values_mut() {
            list.push(name.clone());
        }
    }
    ctx.set_fonts(defs.clone());
    ctx.request_repaint();
}

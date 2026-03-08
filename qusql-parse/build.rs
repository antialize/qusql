use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

#[derive(Debug, Clone)]
struct Keyword {
    name: String,
    properties: HashSet<String>,
    in_restrict_set: bool,
    idx: usize,
}

#[derive(Debug, Clone)]
struct RestrictSet {
    name: String,
    keywords: HashSet<String>,
    bits: u64,
}

fn read_keywords_from_keywords_rs(path: &Path) -> Vec<Keyword> {
    let file = File::open(path).expect("Failed to open src/keywords.rs");
    let reader = BufReader::new(file);
    let mut keywords = Vec::new();
    let mut in_keyword_list = false;

    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        let trimmed = line.trim();

        // Check for start/end markers
        if trimmed.starts_with("// Start of keyword list") {
            in_keyword_list = true;
            continue;
        }
        if trimmed.starts_with("// End of keyword list") {
            break;
        }

        if !in_keyword_list {
            continue;
        }

        // Check if line is an enum variant (indented with 4 spaces)
        if !line.starts_with("    ") {
            continue;
        }

        let trimmed = line.trim();

        // Skip comments
        if trimmed.starts_with("//") {
            continue;
        }

        // Skip attributes like #[default]
        if trimmed.starts_with("#[") {
            continue;
        }

        // Parse enum variant line like: "ADD, // reserved: mariadb, sqlite"
        // Split on comma to get variant name and potential comment
        let Some((name, rem)) = trimmed.split_once(',') else {
            continue;
        };

        // Skip special cases
        if name == "NOT_A_KEYWORD" || name == "QUOTED_IDENTIFIER" {
            continue;
        }

        // Empty name means we hit a line we shouldn't parse
        if name.is_empty() {
            continue;
        }

        let name = name.trim().to_string();

        // Parse comment if present
        let mut properties = HashSet::new();
        if let Some((_, comment)) = rem.split_once("//") {
            let comment = comment.trim();

            // Parse properties from comment
            for part in comment.split(';') {
                let part = part.trim();
                if let Some(reserved_types) = part.strip_prefix("reserved:") {
                    let reserved_types = reserved_types.trim();
                    for reserve_type in reserved_types.split(',') {
                        let reserve_type = reserve_type.trim();
                        properties.insert(format!("{}_reserved", reserve_type));
                    }
                } else if part == "expr_ident" {
                    properties.insert("expr_ident".to_string());
                }
            }
        }

        keywords.push(Keyword {
            name,
            properties,
            in_restrict_set: false,
            idx: keywords.len(),
        });
    }

    keywords
}

fn read_restrict_sets_from_keywords_rs(path: &Path) -> Vec<RestrictSet> {
    let file = File::open(path).expect("Failed to open src/keywords.rs");
    let reader = BufReader::new(file);
    let mut restrict_sets = Vec::new();
    let mut in_restrict_list = false;
    let mut current_doc_comment = None;

    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        let trimmed = line.trim();

        // Check for start/end markers
        if trimmed.starts_with("// Start restrict set list") {
            in_restrict_list = true;
            continue;
        }
        if trimmed.starts_with("// End restrict set list") {
            break;
        }

        if !in_restrict_list {
            continue;
        }

        // Parse doc comments like: /// Restrict keywords: USING, WITH
        if let Some(doc_start) = trimmed.strip_prefix("///") {
            if let Some(keywords_part) = doc_start.trim().strip_prefix("Restrict keywords:") {
                let keywords: HashSet<String> = keywords_part
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                current_doc_comment = Some(keywords);
            }
            continue;
        }

        // Parse const definition like: pub const USING: Self = Restrict(0b1000);
        if trimmed.starts_with("pub const ")
            && let Some(keywords) = current_doc_comment.take()
            && let Some(rest) = trimmed.strip_prefix("pub const ")
            && let Some((name, rem)) = rest.split_once(':')
            && let Some((_, tail)) = rem.split_once("Restrict(")
            && let Some((number, _)) = tail.split_once(')')
        {
            let number = number.trim();
            let bits = if let Some(stripped) = number.strip_prefix("0b") {
                u64::from_str_radix(stripped, 2).unwrap_or(0)
            } else if let Some(stripped) = number.strip_prefix("0x") {
                u64::from_str_radix(stripped, 16).unwrap_or(0)
            } else {
                number.parse::<u64>().unwrap_or(0)
            };

            restrict_sets.push(RestrictSet {
                name: name.trim().to_string(),
                keywords,
                bits,
            });
        }
    }

    restrict_sets
}

fn c_pat(c: char) -> String {
    if c.is_ascii_alphabetic() && c.is_uppercase() {
        let lower = c.to_lowercase().next().unwrap();
        format!("b'{}' | b'{}'", c, lower)
    } else {
        format!("b'{}'", c)
    }
}

fn generate_kw_match(
    out: &mut dyn Write,
    i: usize,
    keywords: &[&Keyword],
    indent_base: usize,
) -> std::io::Result<()> {
    let indent = "    ".repeat(indent_base);

    if let [kw] = keywords {
        let remaining = &kw.name[i..];

        if remaining.is_empty() {
            writeln!(out, "{}if cs.next().is_none() {{", indent)?;
        } else {
            let chars: Vec<char> = remaining.chars().collect();
            write!(out, "{}if ", indent)?;
            for (j, c) in chars.iter().enumerate() {
                if j == 0 {
                    writeln!(out, "matches!(cs.next(), Some({}))", c_pat(*c))?;
                } else {
                    writeln!(
                        out,
                        "{}  && matches!(cs.next(), Some({}))",
                        indent,
                        c_pat(*c)
                    )?;
                }
            }
            writeln!(out, "{}  && cs.next().is_none() {{", indent)?;
        }
        writeln!(out, "{}    Keyword::{}", indent, kw.name)?;
        writeln!(out, "{}}} else {{", indent)?;
        writeln!(out, "{}    Keyword::NOT_A_KEYWORD", indent)?;
        writeln!(out, "{}}}", indent)?;
    } else {
        writeln!(out, "{}match cs.next() {{", indent)?;

        let mut s = 0;
        let mut e = 0;

        while e <= keywords.len() {
            let c = keywords.get(s).and_then(|kw| kw.name.chars().nth(i));

            let next_c = keywords.get(e).and_then(|kw| kw.name.chars().nth(i));

            if e < keywords.len() && next_c == c {
                e += 1;
                continue;
            }

            if let Some(ch) = c {
                writeln!(out, "{}    Some({}) => {{", indent, c_pat(ch))?;
                generate_kw_match(out, i + 1, &keywords[s..e], indent_base + 2)?;
                writeln!(out, "{}    }}", indent)?;
            } else if s < keywords.len() {
                writeln!(out, "{}    None => Keyword::{},", indent, keywords[s].name)?;
            }

            s = e;
            e += 1;
        }

        writeln!(out, "{}    _ => Keyword::NOT_A_KEYWORD,", indent)?;
        writeln!(out, "{}}}", indent)?;
    }

    Ok(())
}

fn main() {
    println!("cargo:rerun-if-changed=src/keywords.rs");

    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir);

    // Read input files
    let mut keywords = read_keywords_from_keywords_rs(Path::new("src/keywords.rs"));

    keywords.sort_by(|a, b| a.name.cmp(&b.name));

    let keyword_indexes: HashMap<String, usize> = keywords
        .iter()
        .enumerate()
        .map(|(i, kw)| (kw.name.clone(), i))
        .collect();

    // Build reserved keyword sets
    let mut mariadb_restrict = HashSet::new();
    let mut postgres_restrict = HashSet::new();
    let mut sqlite_restrict = HashSet::new();

    for kw in &keywords {
        if kw.properties.contains("mariadb_reserved") {
            mariadb_restrict.insert(kw.name.clone());
        }
        if kw.properties.contains("postgres_reserved") {
            postgres_restrict.insert(kw.name.clone());
        }
        if kw.properties.contains("sqlite_reserved") {
            sqlite_restrict.insert(kw.name.clone());
        }
    }

    // Read custom restrict sets from keywords.rs and add the base ones with hardcoded bits
    let mut restrict_sets = read_restrict_sets_from_keywords_rs(Path::new("src/keywords.rs"));

    // Add base restrict sets with hardcoded bits
    restrict_sets.push(RestrictSet {
        name: "MARIADB".to_string(),
        keywords: mariadb_restrict,
        bits: 0b1,
    });
    restrict_sets.push(RestrictSet {
        name: "POSTGRES".to_string(),
        keywords: postgres_restrict,
        bits: 0b10,
    });
    restrict_sets.push(RestrictSet {
        name: "SQLITE".to_string(),
        keywords: sqlite_restrict,
        bits: 0b100,
    });

    // Mark keywords that are in restrict sets
    for rs in &restrict_sets {
        for kw_name in &rs.keywords {
            if let Some(&idx) = keyword_indexes.get(kw_name) {
                keywords[idx].in_restrict_set = true;
            } else {
                panic!(
                    "Keyword {} in restrict set {} is not defined in keywords.rs",
                    kw_name, rs.name
                );
            }
        }
    }

    // ============================================================
    // Generate keyword_gen.rs - Constants and data arrays
    // ============================================================
    let mut out =
        File::create(out_path.join("keyword_gen.rs")).expect("Failed to create keyword_gen.rs");

    let mut restrict_keywords = Vec::new();
    for k in &keywords {
        if !k.in_restrict_set {
            continue;
        }
        while restrict_keywords.len() <= k.idx {
            restrict_keywords.push(None);
        }
        restrict_keywords[k.idx] = Some(k);
    }

    writeln!(out, "use crate::keywords::{{Keyword, Restrict}};").unwrap();
    writeln!(
        out,
        "pub(crate) const KEYWORDS_RESTRICT: [Restrict; {}] = [",
        restrict_keywords.len()
    )
    .unwrap();

    for knw in &restrict_keywords {
        let mut bits = 0;
        if let Some(kw) = knw {
            for rs in &restrict_sets {
                if rs.keywords.contains(&kw.name) {
                    bits |= rs.bits;
                }
            }
            writeln!(out, "    Restrict(0x{:x}), // {}", bits, kw.name).unwrap();
        } else {
            writeln!(out, "    Restrict(0), // Skip").unwrap();
        }
    }

    writeln!(out, "];").unwrap();
    writeln!(out).unwrap();

    // Generate name() method body
    writeln!(
        out,
        "pub(crate) fn keyword_name(kw: Keyword) -> &'static str {{"
    )
    .unwrap();
    writeln!(out, "    match kw {{").unwrap();
    writeln!(out, "        Keyword::NOT_A_KEYWORD => \"NOT_A_KEYWORD\",").unwrap();
    writeln!(
        out,
        "        Keyword::QUOTED_IDENTIFIER => \"QUOTED_IDENTIFIER\","
    )
    .unwrap();
    for kw in &keywords {
        writeln!(out, "        Keyword::{} => \"{}\",", kw.name, kw.name).unwrap();
    }
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    writeln!(out).unwrap();

    // Generate expr_ident() method body
    writeln!(
        out,
        "pub(crate) const fn keyword_expr_ident(kw: Keyword) -> bool {{"
    )
    .unwrap();
    writeln!(out, "    matches!(kw,").unwrap();

    let mut first = true;
    for kw in &keywords {
        if !kw.properties.contains("expr_ident") {
            continue;
        }
        if !first {
            writeln!(out, "        | Keyword::{}", kw.name).unwrap();
        } else {
            first = false;
            writeln!(out, "        Keyword::{}", kw.name).unwrap();
        }
    }

    writeln!(out, "    )").unwrap();
    writeln!(out, "}}").unwrap();

    writeln!(out, "pub(crate) fn keyword_from_str(v: &str) -> Keyword {{").unwrap();
    writeln!(out, "    let mut cs = v.as_bytes().iter();").unwrap();

    let kw_refs: Vec<&Keyword> = keywords.iter().collect();
    generate_kw_match(&mut out, 0, &kw_refs, 2).unwrap();

    writeln!(out, "}}").unwrap();

    println!("cargo:rustc-env=KEYWORDS_GENERATED=1");
}

//! UniFFI-friendly Markdown AST values.
//!
//! The parser crate owns the real AST. These records/enums keep the generated
//! Swift/Kotlin surface stable and host-friendly.

use marmot_markdown::{
    Alignment as MdAlignment, AutolinkKind as MdAutolinkKind, Block as MdBlock,
    CodeBlockKind as MdCodeBlockKind, Document as MdDocument, Inline as MdInline,
    ListItem as MdListItem, ListKind as MdListKind, NostrEntity as MdNostrEntity,
    NostrHrp as MdNostrHrp, TableCell as MdTableCell,
};

#[derive(Clone, Debug, Default, PartialEq, Eq, uniffi::Record)]
pub struct MarkdownDocumentFfi {
    pub blocks: Vec<MarkdownBlockFfi>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MarkdownBlockFfi {
    Paragraph {
        inlines: Vec<MarkdownInlineFfi>,
    },
    Heading {
        level: u8,
        inlines: Vec<MarkdownInlineFfi>,
    },
    ThematicBreak,
    CodeBlock {
        kind: MarkdownCodeBlockKindFfi,
        info: String,
        content: String,
    },
    BlockQuote {
        blocks: Vec<MarkdownBlockFfi>,
    },
    List {
        kind: MarkdownListKindFfi,
        tight: bool,
        items: Vec<MarkdownListItemFfi>,
    },
    Table {
        alignments: Vec<MarkdownAlignmentFfi>,
        header: Vec<MarkdownTableCellFfi>,
        rows: Vec<Vec<MarkdownTableCellFfi>>,
    },
    MathBlock {
        content: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MarkdownCodeBlockKindFfi {
    Indented,
    Fenced,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MarkdownListKindFfi {
    /// `marker` is a single-character string: "-", "*", or "+".
    Bullet { marker: String },
    /// `delimiter` is a single-character string: "." or ")".
    Ordered { start: u32, delimiter: String },
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MarkdownListItemFfi {
    pub blocks: Vec<MarkdownBlockFfi>,
    /// `None` for plain bullets/ordered items, `Some(false)` for `[ ]`,
    /// `Some(true)` for `[x]`.
    pub checked: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MarkdownAlignmentFfi {
    None,
    Left,
    Center,
    Right,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MarkdownTableCellFfi {
    pub inlines: Vec<MarkdownInlineFfi>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MarkdownInlineFfi {
    Text {
        content: String,
    },
    SoftBreak,
    HardBreak,
    Code {
        content: String,
    },
    Emph {
        children: Vec<MarkdownInlineFfi>,
    },
    Strong {
        children: Vec<MarkdownInlineFfi>,
    },
    Strikethrough {
        children: Vec<MarkdownInlineFfi>,
    },
    Link {
        dest: String,
        title: Option<String>,
        children: Vec<MarkdownInlineFfi>,
    },
    Image {
        dest: String,
        title: Option<String>,
        alt: Vec<MarkdownInlineFfi>,
    },
    Autolink {
        url: String,
        kind: MarkdownAutolinkKindFfi,
    },
    Math {
        content: String,
    },
    NostrMention {
        entity: MarkdownNostrEntityFfi,
    },
    NostrUri {
        entity: MarkdownNostrEntityFfi,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MarkdownAutolinkKindFfi {
    Uri,
    Email,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MarkdownNostrEntityFfi {
    pub hrp: MarkdownNostrHrpFfi,
    pub bech32: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MarkdownNostrHrpFfi {
    Npub,
    Note,
    Nevent,
    Nprofile,
    Naddr,
    Nrelay,
}

pub(crate) fn parse_markdown_document(text: &str) -> MarkdownDocumentFfi {
    marmot_markdown::parse(text).into()
}

impl From<&MdDocument> for MarkdownDocumentFfi {
    fn from(value: &MdDocument) -> Self {
        Self {
            blocks: value.blocks.iter().map(MarkdownBlockFfi::from).collect(),
        }
    }
}

impl From<MdDocument> for MarkdownDocumentFfi {
    fn from(value: MdDocument) -> Self {
        (&value).into()
    }
}

impl From<&MdBlock> for MarkdownBlockFfi {
    fn from(value: &MdBlock) -> Self {
        match value {
            MdBlock::Paragraph { inlines } => Self::Paragraph {
                inlines: inlines.iter().map(MarkdownInlineFfi::from).collect(),
            },
            MdBlock::Heading { level, inlines } => Self::Heading {
                level: *level,
                inlines: inlines.iter().map(MarkdownInlineFfi::from).collect(),
            },
            MdBlock::ThematicBreak => Self::ThematicBreak,
            MdBlock::CodeBlock {
                kind,
                info,
                content,
            } => Self::CodeBlock {
                kind: (*kind).into(),
                info: info.clone(),
                content: content.clone(),
            },
            MdBlock::BlockQuote { blocks } => Self::BlockQuote {
                blocks: blocks.iter().map(MarkdownBlockFfi::from).collect(),
            },
            MdBlock::List { kind, tight, items } => Self::List {
                kind: kind.into(),
                tight: *tight,
                items: items.iter().map(MarkdownListItemFfi::from).collect(),
            },
            MdBlock::Table {
                alignments,
                header,
                rows,
            } => Self::Table {
                alignments: alignments
                    .iter()
                    .map(|alignment| (*alignment).into())
                    .collect(),
                header: header.iter().map(MarkdownTableCellFfi::from).collect(),
                rows: rows
                    .iter()
                    .map(|row| row.iter().map(MarkdownTableCellFfi::from).collect())
                    .collect(),
            },
            MdBlock::MathBlock { content } => Self::MathBlock {
                content: content.clone(),
            },
        }
    }
}

impl From<MdCodeBlockKind> for MarkdownCodeBlockKindFfi {
    fn from(value: MdCodeBlockKind) -> Self {
        match value {
            MdCodeBlockKind::Indented => Self::Indented,
            MdCodeBlockKind::Fenced => Self::Fenced,
        }
    }
}

impl From<&MdListKind> for MarkdownListKindFfi {
    fn from(value: &MdListKind) -> Self {
        match *value {
            MdListKind::Bullet { marker } => Self::Bullet {
                marker: (marker as char).to_string(),
            },
            MdListKind::Ordered { start, delimiter } => Self::Ordered {
                start,
                delimiter: (delimiter as char).to_string(),
            },
        }
    }
}

impl From<&MdListItem> for MarkdownListItemFfi {
    fn from(value: &MdListItem) -> Self {
        Self {
            blocks: value.blocks.iter().map(MarkdownBlockFfi::from).collect(),
            checked: value.checked,
        }
    }
}

impl From<MdAlignment> for MarkdownAlignmentFfi {
    fn from(value: MdAlignment) -> Self {
        match value {
            MdAlignment::None => Self::None,
            MdAlignment::Left => Self::Left,
            MdAlignment::Center => Self::Center,
            MdAlignment::Right => Self::Right,
        }
    }
}

impl From<&MdTableCell> for MarkdownTableCellFfi {
    fn from(value: &MdTableCell) -> Self {
        Self {
            inlines: value.inlines.iter().map(MarkdownInlineFfi::from).collect(),
        }
    }
}

impl From<&MdInline> for MarkdownInlineFfi {
    fn from(value: &MdInline) -> Self {
        match value {
            MdInline::Text(content) => Self::Text {
                content: content.clone(),
            },
            MdInline::SoftBreak => Self::SoftBreak,
            MdInline::HardBreak => Self::HardBreak,
            MdInline::Code(content) => Self::Code {
                content: content.clone(),
            },
            MdInline::Emph(children) => Self::Emph {
                children: children.iter().map(MarkdownInlineFfi::from).collect(),
            },
            MdInline::Strong(children) => Self::Strong {
                children: children.iter().map(MarkdownInlineFfi::from).collect(),
            },
            MdInline::Strikethrough(children) => Self::Strikethrough {
                children: children.iter().map(MarkdownInlineFfi::from).collect(),
            },
            MdInline::Link {
                dest,
                title,
                children,
            } => Self::Link {
                dest: dest.clone(),
                title: title.clone(),
                children: children.iter().map(MarkdownInlineFfi::from).collect(),
            },
            MdInline::Image { dest, title, alt } => Self::Image {
                dest: dest.clone(),
                title: title.clone(),
                alt: alt.iter().map(MarkdownInlineFfi::from).collect(),
            },
            MdInline::Autolink { url, kind } => Self::Autolink {
                url: url.clone(),
                kind: (*kind).into(),
            },
            MdInline::Math(content) => Self::Math {
                content: content.clone(),
            },
            MdInline::NostrMention(entity) => Self::NostrMention {
                entity: entity.into(),
            },
            MdInline::NostrUri(entity) => Self::NostrUri {
                entity: entity.into(),
            },
        }
    }
}

impl From<MdAutolinkKind> for MarkdownAutolinkKindFfi {
    fn from(value: MdAutolinkKind) -> Self {
        match value {
            MdAutolinkKind::Uri => Self::Uri,
            MdAutolinkKind::Email => Self::Email,
        }
    }
}

impl From<&MdNostrEntity> for MarkdownNostrEntityFfi {
    fn from(value: &MdNostrEntity) -> Self {
        Self {
            hrp: value.hrp.into(),
            bech32: value.bech32.clone(),
        }
    }
}

impl From<MdNostrHrp> for MarkdownNostrHrpFfi {
    fn from(value: MdNostrHrp) -> Self {
        match value {
            MdNostrHrp::Npub => Self::Npub,
            MdNostrHrp::Note => Self::Note,
            MdNostrHrp::Nevent => Self::Nevent,
            MdNostrHrp::Nprofile => Self::Nprofile,
            MdNostrHrp::Naddr => Self::Naddr,
            MdNostrHrp::Nrelay => Self::Nrelay,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_document() {
        assert_eq!(parse_markdown_document(""), MarkdownDocumentFfi::default());
    }

    #[test]
    fn bridges_emphasis_strike_and_link() {
        let document = parse_markdown_document("**bold** ~~gone~~ [site](https://example.com)");
        let MarkdownBlockFfi::Paragraph { inlines } = &document.blocks[0] else {
            panic!("expected paragraph");
        };
        assert!(matches!(inlines[0], MarkdownInlineFfi::Strong { .. }));
        assert!(matches!(
            inlines[2],
            MarkdownInlineFfi::Strikethrough { .. }
        ));
        assert!(matches!(
            inlines[4],
            MarkdownInlineFfi::Link { ref dest, .. } if dest == "https://example.com"
        ));
    }

    #[test]
    fn bridges_nostr_entities() {
        let body = "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq";
        let document = parse_markdown_document(&format!("@npub1{body} nostr:npub1{body}"));
        let MarkdownBlockFfi::Paragraph { inlines } = &document.blocks[0] else {
            panic!("expected paragraph");
        };
        assert!(matches!(
            inlines[0],
            MarkdownInlineFfi::NostrMention {
                entity: MarkdownNostrEntityFfi {
                    hrp: MarkdownNostrHrpFfi::Npub,
                    ..
                }
            }
        ));
        assert!(matches!(
            inlines[2],
            MarkdownInlineFfi::NostrUri {
                entity: MarkdownNostrEntityFfi {
                    hrp: MarkdownNostrHrpFfi::Npub,
                    ..
                }
            }
        ));
    }

    #[test]
    fn bridges_darkmatter_autolink() {
        let document = parse_markdown_document("open darkmatter://profile/npub1abc");
        let MarkdownBlockFfi::Paragraph { inlines } = &document.blocks[0] else {
            panic!("expected paragraph");
        };
        assert!(matches!(
            inlines[1],
            MarkdownInlineFfi::Autolink { ref url, .. } if url == "darkmatter://profile/npub1abc"
        ));
    }

    #[test]
    fn bridges_table() {
        let document = parse_markdown_document("| a | b |\n| :- | -: |\n| 1 | 2 |");
        assert!(matches!(
            document.blocks[0],
            MarkdownBlockFfi::Table {
                ref alignments,
                ref header,
                ref rows,
            } if alignments == &[MarkdownAlignmentFfi::Left, MarkdownAlignmentFfi::Right]
                && header.len() == 2
                && rows.len() == 1
        ));
    }
}

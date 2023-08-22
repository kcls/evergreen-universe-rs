use eg::common::bib;
use eg::editor::Editor;
use eg::result::EgResult;
use evergreen as eg;

fn main() -> EgResult<()> {
    let ctx = eg::init::init()?;
    let client = ctx.client();
    let mut editor = Editor::new(client, ctx.idl());

    let bib_ids = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    let display_attrs = bib::get_display_attrs(&mut editor, bib_ids)?;

    for (bib_id, map) in display_attrs {
        if let Some(title) = map.get("title") {
            println!("Bib {bib_id} title is {}", title.first());
        }
        if let Some(author) = map.get("author") {
            println!("Bib {bib_id} author is {}", author.first());
        }
        println!("--");
    }

    Ok(())
}

use eg::common::bib;
use eg::editor::Editor;
use eg::result::EgResult;
use evergreen as eg;

fn main() -> EgResult<()> {
    let ctx = eg::init::init()?;
    let client = ctx.client();
    let mut editor = Editor::new(client, ctx.idl());

    let bib_ids = &[1, 2, 3];

    let display_attrs = bib::get_display_attrs(&mut editor, bib_ids)?;

    for (bib_id, attr_set) in display_attrs {
        println!("TITLE: {}", attr_set.first_value("title"));

        for attr in attr_set.attrs() {
            println!("Bib {bib_id} [{}] ({}) => {}",
                attr.name(), attr.label(), attr.value().first());
        }
    }

    Ok(())
}

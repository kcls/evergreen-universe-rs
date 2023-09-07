use eg::common::bib;
use eg::common::holds;
use eg::common::targeter;
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
            println!(
                "Bib {bib_id} [{}] ({}) => {}",
                attr.name(),
                attr.label(),
                attr.value().first()
            );
        }
    }

    let related = holds::related_to_copy(&mut editor, 3000, 4).unwrap();
    for hold in related {
        println!("related hold: {hold}");
    }

    let mut tgtr = targeter::HoldTargeter::new(editor.clone());

    // Retarget some holds.
    for hold_id in 1..10 {
        let ctx = tgtr.target_hold(hold_id, None)?;
        println!("{hold_id} target success={}", ctx.success());
    }

    Ok(())
}

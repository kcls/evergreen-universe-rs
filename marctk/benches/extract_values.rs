use criterion::{criterion_group, criterion_main, Criterion};
use marctk::Record;

pub fn extract_values_benchmark(c: &mut Criterion) {
    let record = Record::from_breaker(
        r#"=600 10$aZhang, Heng, $d 78-139 $v Juvenile literature.
=650 \0$aAmusement parks $vComic books, strips, etc.
=655 \7$aHorror comics. $2lcgft
=655 \7$aGraphic novels. $2lcgft"#,
    )
    .unwrap();
    let genre_query = "600(*0)vx:650(*0)vx:655(*0)avx:655(*7)avx";
    c.bench_function("extract_values", |b| {
        b.iter(|| {
            let genres = record.extract_values(genre_query);
            assert!(!genres.is_empty())
        })
    });
}

criterion_group!(benches, extract_values_benchmark);
criterion_main!(benches);

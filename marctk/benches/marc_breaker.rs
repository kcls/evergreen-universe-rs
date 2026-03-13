use criterion::{criterion_group, criterion_main, Criterion};
use marctk::Record;

fn from_breaker_benchmark(c: &mut Criterion) {
    let breaker = r#"=LDR 01703cas a2200433 a 4500
=005 20200728155605.0
=008 810303d19741985nyubr p       0   a0eng d
=001 991583506421
=010 \\$asc 86007020 $zsn 85011289
=035 \\$a(OCoLC)ocm04522925$0(uri) http://www.worldcat.org/oclc/04522925
=035 \\$a(CStRLIN)NJPG0361-S
=035 \\$9AAF4392TS
=035 \\$a158
=035 \\$a(NjP)158-princetondb
=040 \\$aXQM$beng$cXQM$dNYG$dHUL$dMUL$dIUL$dNST$dDLC$dHUL$dNYG$dNST$dNSD$dNST$dIUL$dOCL
=022 0\$a0898-1078$0(uri) http://worldcat.org/issn/0898-1078
=042 \\$alc$ansdp
=043 \\$an-us---
=082 10$a686$211
=210 0\$aAPHA lett.
=222 \4$aThe APHA letter
=245 04$aThe APHA letter /$cAmerican Printing History Association.
=246 3\$aAmerican Printing History Association letter
=260 \\$a[New York] :$bThe Association,$c[1974-1985]
=265 \\$aAmerican Printing History Association, P.O. Box 4922, Grand Central Station, New York, NY 10017
=300 \\$a12 v. ;$c28 cm.
=310 \\$aSix no. yearly
=362 0\$aNo. 1 (Nov. 1974)-no. 68.
=500 \\$aTitle from caption.
=515 \\$aNo. 27-68 also numbered 1979, no. 1-1985, no. 6.
=515 \\$aNo. 1 re-issued as reprint in Feb. 1975.
=525 \\$aHas occasional supplements.
=555 \\$aNo. 1 (1974)-8 (1975), with no. 8.
=650 \0$aPrinting$zUnited States$xHistory$vPeriodicals.$0(uri) http://id.loc.gov/authorities/subjects/sh85106751
=650 \0$aPrinting$vPeriodicals.$0http://id.loc.gov/authorities/subjects/sh85106764$0(uri) http://id.loc.gov/authorities/subjects/sh85106764
=610 20$aAmerican Printing History Association$vPeriodicals.$0(uri) http://id.loc.gov/authorities/names/n79074225$0(uri) http://viaf.org/viaf/sourceID/LC|n79074225
=655 \7$aPeriodicals.$2lcgft$0http://id.loc.gov/authorities/genreForms/gf2014026139$0(uri) http://id.loc.gov/authorities/genreForms/gf2014026139
=710 2\$aAmerican Printing History Association.$0http://id.loc.gov/authorities/names/n79074225$0(uri) http://id.loc.gov/authorities/names/n79074225$0(uri) http://viaf.org/viaf/sourceID/LC|n79074225
=785 00$tAPHA newsletter$w(DLC)sn 86021679$w(OCoLC)13154156
=999 \\$a158
=950 \\$c2021-08-03 16:34:55 US/Eastern$b2021-07-13 08:24:26 US/Eastern$afalse
=852 81$brare$cga$h2006-0030Q$kOversize$822740217130006421
=866 \0$aNo. 1 (Feb. 1975)-no. 68$822740217130006421
=866 \0$zLACKS: no. 25-26,29,49-50, 56$822740217130006421
=867 \0$ano. 20$822740217130006421
=952 \\$a2021-07-13 12:24:26$822740217130006421$bSpecial Collections$cga: Graphic Arts Collection$efalse
=876 \\$022740217130006421$a23740217110006421$j1$zga$3no.46-68 (inc.)$d2021-07-13 12:24:26$p32101070758865$t1$yrare
=876 \\$022740217130006421$a23740217120006421$j1$zga$3no.1-45 (inc.)$d2021-07-13 12:24:26$p32101036897518$t1$yrare"#;
    c.bench_function("from_breaker", |b| {
        b.iter(|| {
            let record = Record::from_breaker(breaker).unwrap();
            assert_eq!(
                record.get_control_fields("001").first().unwrap().content(),
                "991583506421"
            );
        });
    });
}

criterion_group!(benches, from_breaker_benchmark);
criterion_main!(benches);

use clap::Parser;
use geo::Contains;
use shapefile::{Shape, Point};
use std::path::Path;

#[derive(Parser, Debug)]
#[command(
    author, 
    version, 
    about = "Test if a shapefile contains a given lat/long point",
    long_about = "Test if a shapefile contains a given lat/long point.\n\nNote: When passing negative numbers (e.g., negative longitude), use the equals sign format to prevent parsing issues.\nExample: test-shapefile-point --lat 47.6062 --long=-122.3321 --shapefile file.shp"
)]
struct Args {
    /// Latitude
    #[arg(short = 'a', long)]
    lat: f64,

    /// Longitude
    #[arg(short = 'o', long)]
    long: f64,

    /// Path to shapefile
    #[arg(short = 's', long)]
    shapefile: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if !Path::new(&args.shapefile).exists() {
        eprintln!("Error: Shapefile does not exist: {}", args.shapefile);
        std::process::exit(1);
    }

    let contains = shapefile_contains(&args.shapefile, args.lat, args.long)?;
    
    println!("Shapefile: {}", args.shapefile);
    println!("Latitude: {}", args.lat);
    println!("Longitude: {}", args.long);
    println!("Contains point: {}", if contains { "YES" } else { "NO" });

    Ok(())
}

/// Returns true if the shapefile provided contains the lat/long provided.
fn shapefile_contains(shapefile: &str, lat: f64, long: f64) -> Result<bool, Box<dyn std::error::Error>> {
    eprintln!("Inspecting shapefile {} for lat={} and long={}", shapefile, lat, long);

    let mut reader = shapefile::Reader::from_path(shapefile)?;

    let point = Point::new(long, lat); // Note: shapefile uses x,y (long,lat) order
    
    for shape_record in reader.iter_shapes_and_records() {
        let (shape, _record) = shape_record?;

        if let Shape::Polygon(poly) = shape {
            let geo_poly: geo::MultiPolygon<f64> = poly.into();
            let geo_point: geo::Point<f64> = point.into();

            if geo_poly.contains(&geo_point) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

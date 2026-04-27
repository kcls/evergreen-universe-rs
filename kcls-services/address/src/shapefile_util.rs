use geo::prelude::Contains;
use shapefile::record::*;
use std::path::Path;

/// Returns true if the shapefile at the given path contains the provided lat/long.
pub fn shapefile_contains(shapefile: &str, lat: f64, long: f64) -> Result<bool, String> {
    log::debug!("Inspecting shapefile {shapefile} for lat={lat} and long={long}");

    if !Path::new(shapefile).exists() {
        log::debug!("No such shapefile: {shapefile}");
        return Ok(false);
    }

    let mut reader = shapefile::Reader::from_path(shapefile)
        .map_err(|e| format!("Cannot read shapefile: {shapefile}: {e}"))?;

    let point = Point::new(long, lat); // x, y

    for shape_record in reader.iter_shapes_and_records() {
        let (shape, _record) = shape_record
            .map_err(|e| format!("Cannot extract shape/record from {shapefile}: {e}"))?;

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

# Shapefile Point Containment Test Utility

This utility tests whether a given latitude/longitude point is contained within a shapefile.

## Building

```bash
cargo build --package eg-service-addrs --bin test-shapefile-point
```

For a release (optimized) build:
```bash
cargo build --release --package eg-service-addrs --bin test-shapefile-point
```

## Usage

```bash
test-shapefile-point --lat <latitude> --long <longitude> --shapefile <path/to/shapefile.shp>
```

**Important Note:** When passing negative numbers (such as negative longitude values for western hemisphere locations), use the equals sign format to prevent the parser from interpreting them as flags:

```bash
test-shapefile-point --lat 47.6062 --long=-122.3321 --shapefile file.shp
```

### Using cargo run:
```bash
cargo run --package eg-service-addrs --bin test-shapefile-point -- --lat <latitude> --long <longitude> --shapefile <path/to/shapefile.shp>
```

With negative longitude:
```bash
cargo run --package eg-service-addrs --bin test-shapefile-point -- --lat 47.6062 --long=-122.3321 --shapefile file.shp
```

### Using the compiled binary:
```bash
./target/debug/test-shapefile-point --lat <latitude> --long <longitude> --shapefile <path/to/shapefile.shp>
```

With negative longitude:
```bash
./target/debug/test-shapefile-point --lat 47.6062 --long=-122.3321 --shapefile file.shp
```

## Examples

Test if a point in Seattle (47.6062, -122.3321) is within the KL shapefile:
```bash
./target/debug/test-shapefile-point --lat 47.6062 --long=-122.3321 --shapefile kcls-services/addrs/data/shapefiles/home-orgs/KL/KL.shp
```

Test if a point in Auburn (47.3073, -122.2285) is within the AU shapefile:
```bash
./target/debug/test-shapefile-point --lat 47.3073 --long=-122.2285 --shapefile kcls-services/addrs/data/shapefiles/home-orgs/AU/AU.shp
```

Using cargo run:
```bash
cargo run --package eg-service-addrs --bin test-shapefile-point -- --lat 47.3073 --long=-122.2285 --shapefile kcls-services/addrs/data/shapefiles/home-orgs/AU/AU.shp
```

Short form using -a (lat), -o (long), -s (shapefile) flags:
```bash
./target/debug/test-shapefile-point -a 47.3073 -o=-122.2285 -s kcls-services/addrs/data/shapefiles/home-orgs/AU/AU.shp
```

## Output

The utility will output:
- The shapefile path
- The latitude and longitude being tested
- Whether the point is contained in the shapefile (YES/NO)

Example output:
```
Shapefile: data/shapefiles/home-orgs/KL/KL.shp
Latitude: 47.6062
Longitude: -122.3321
Contains point: YES
```

## Notes

- The shapefile must exist or the utility will exit with an error
- The utility expects standard shapefile format (.shp file)
- Coordinates should be in decimal degrees (WGS84)
- Longitude is negative for western hemisphere locations
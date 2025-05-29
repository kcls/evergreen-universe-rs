# Home Org Test Data

This directory contains test data for the `open-ils.rs-addrs.home-org` API tests.

## File: home_org_test_locations.json

### Structure

The JSON file contains test locations organized into several categories:

#### test_locations
Grouped by geographic area within Washington State:
- **king_county_seattle**: Seattle neighborhoods (10 locations)
- **king_county_eastside**: Eastside cities like Bellevue, Redmond (8 locations)
- **king_county_south**: South King County cities (8 locations)
- **king_county_north**: North King County cities (4 locations)
- **other_washington**: Major cities outside King County (10 locations)

#### boundary_tests
Test points at the borders of King County (4 locations)

#### edge_cases
Unusual or remote locations to test edge cases (5 locations)

#### invalid_coordinates
Locations outside Washington State to test error handling (5 locations)

### Location Object Format

```json
{
  "name": "Location Name",
  "lat": 47.6062,
  "lon": -122.3321,
  "expected_org": 1492,  // null if no org unit expected
  "notes": "Optional notes about the location"
}
```

### Expected Org Units

The following locations have verified org unit IDs:
- All King County eastside locations (8)
- Most King County south locations (7 of 8)
- All King County north locations (4)

Total: 19 locations with expected org units

### Usage

The test data is loaded by `home_org_coverage.rs` tests using serde_json deserialization.

To add new test locations:
1. Add to the appropriate geographic group
2. Include lat/lon coordinates
3. Set expected_org to null initially
4. Run tests to discover the actual org unit
5. Update expected_org with the discovered value if it should be verified

### Notes

- Seattle proper locations currently return no org unit (shapefile coverage issue)
- Locations outside King County return no org unit
- Each successful location maps to a unique library branch org unit ID
package domain

import "math"

// Geo is a point on the surface of the earth, with an optional human-readable
// place name carried only so the dashboard can print "São Paulo" instead of
// a pair of decimals.
type Geo struct {
	Lat   float64 `json:"lat"`
	Lon   float64 `json:"lon"`
	Place string  `json:"place,omitempty"`
}

// earthRadiusKm is the mean radius. The geovelocity rule compares implied
// speed against a threshold an order of magnitude below orbital velocity, so
// the ~0.3% error of a spherical model versus WGS84 is irrelevant here.
const earthRadiusKm = 6371.0

// DistanceKm returns the great-circle distance between two points using the
// haversine formula, which stays numerically stable for the small distances
// where the simpler spherical law of cosines loses precision.
func (g Geo) DistanceKm(o Geo) float64 {
	lat1, lat2 := radians(g.Lat), radians(o.Lat)
	dLat := lat2 - lat1
	dLon := radians(o.Lon - g.Lon)

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)

	// Clamp before Asin: accumulated floating-point error can push `a` a few
	// ulps above 1 for antipodal points, and Asin would return NaN.
	return 2 * earthRadiusKm * math.Asin(math.Sqrt(math.Min(1, a)))
}

func radians(deg float64) float64 { return deg * math.Pi / 180 }

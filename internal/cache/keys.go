package cache

const (
	keyUserPlacesPref = "user:"
	keyUserPlacesSuff = ":places"
)

func ToKeyUserPlaces(userID string) string {
	return keyUserPlacesPref + userID + keyUserPlacesSuff
}

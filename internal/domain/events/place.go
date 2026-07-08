package events

import "time"

type PlaceCreated struct {
	PlaceID   string    `json:"place_id"`
	UserID    string    `json:"user_id"`
	Name      string    `json:"name"`
	Info      string    `json:"info"`
	CreatedAt time.Time `json:"created_at"`
}

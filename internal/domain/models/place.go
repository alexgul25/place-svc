package models

import "time"

type Place struct {
	ID        string // UUID
	UserID    string
	Name      string
	Info      string
	CreatedAt time.Time
}

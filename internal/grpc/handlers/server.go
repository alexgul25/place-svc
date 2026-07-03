package handlersgrpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/alexgul25/place-svc/internal/domain/models"
	"github.com/alexgul25/place-svc/internal/grpc/interceptors"
	placev1 "github.com/alexgul25/protos/gen/go/place/v1"
)

type PlaceService interface {
	AddPlace(ctx context.Context, userID string, name string, info string) (models.Place, error)
	GetUserPlaces(ctx context.Context, userID string) ([]models.Place, error)
}

type serverAPI struct {
	placev1.UnimplementedPlaceServiceServer
	placeService PlaceService
}

func Register(gRPCServer *grpc.Server, placeServer PlaceService) {
	placev1.RegisterPlaceServiceServer(gRPCServer, &serverAPI{placeService: placeServer})
}

func (s *serverAPI) AddPlace(ctx context.Context, in *placev1.AddPlaceRequest) (*placev1.AddPlaceResponse, error) {
	if in.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	userID, ok := interceptors.GetUserIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "user id is required")
	}

	place, err := s.placeService.AddPlace(ctx, userID, in.GetName(), in.GetInfo())
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to add place")
	}

	return &placev1.AddPlaceResponse{
		Id:        place.ID,
		UserId:    place.UserID,
		Name:      place.Name,
		Info:      place.Info,
		CreatedAt: timestamppb.New(place.CreatedAt),
	}, nil
}

func (s *serverAPI) GetUserPlaces(ctx context.Context, in *placev1.GetUserPlacesRequest) (*placev1.GetUserPlacesResponse, error) {
	if in.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user id is required")
	}

	places, err := s.placeService.GetUserPlaces(ctx, in.GetUserId())
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get user places")
	}

	grpcPlaces := make([]*placev1.Place, len(places))
	for i, p := range places {
		grpcPlaces[i] = &placev1.Place{
			Name:      p.Name,
			Info:      p.Info,
			CreatedAt: timestamppb.New(p.CreatedAt),
		}
	}

	return &placev1.GetUserPlacesResponse{Places: grpcPlaces}, nil
}

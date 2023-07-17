import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ArrivalServiceImpl extends ArrivalServiceGrpc.ArrivalServiceImplBase {
    private static final Logger log = LogManager.getLogger(ArrivalServiceImpl.class);
    @Override
    public void consumptionRate(ArrivalRequest request, StreamObserver<ArrivalResponse> responseObserver) {
        log.info("received new rate request {}", request.getArrivalrequest());
        log.info("Arrival is {}", OldWorkloadSkewed.ArrivalRate );
        ArrivalResponse arrival = ArrivalResponse.newBuilder()
                .setArrival(OldWorkloadSkewed.ArrivalRate)
                        .build();

        responseObserver.onNext(arrival);
        responseObserver.onCompleted();

    }
}

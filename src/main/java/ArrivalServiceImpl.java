import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ArrivalServiceImpl extends ArrivalServiceGrpc.ArrivalServiceImplBase {
    private static final Logger log = LogManager.getLogger(ArrivalServiceImpl.class);
    @Override
    public void arrivalRate(ArrivalRequest request, StreamObserver<ArrivalResponse> responseObserver) {
        log.info("received new rate request {}", request.getArrivalrequest());
        log.info("Arrival is {}", OldWorkload.ArrivalRate );
        ArrivalResponse arrival = ArrivalResponse.newBuilder()
                .setArrival(OldWorkload.ArrivalRate * getReplicas())
                        .build();

        responseObserver.onNext(arrival);
        responseObserver.onCompleted();

    }


    static int   getReplicas(){
        try (final KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            return k8s.apps().deployments().inNamespace("default").withName("workload").get().getSpec().getReplicas();
        }
    }

}

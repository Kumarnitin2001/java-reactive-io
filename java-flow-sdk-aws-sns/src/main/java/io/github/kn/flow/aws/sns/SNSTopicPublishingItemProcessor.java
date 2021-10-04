package io.github.kn.flow.aws.sns;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.kn.flow.CompletionStageItemProcessor;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Implements a generic AWS-SNS topic message publisher, where each published message is harvested as
 * a {@link CompletionStageItemProcessor} of upstream published items ({@code onNext}).
 */
public class SNSTopicPublishingItemProcessor implements CompletionStageItemProcessor<String, String> {
    private static final Logger LOG = LogManager.getLogger("SNSTopicPublishingItemProcessor");
    private final SnsAsyncClient snsAsyncClient;
    private final String topicArn;

    /**
     * @param sesAsyncClient
     * @param arn
     */
    public SNSTopicPublishingItemProcessor(final SnsAsyncClient sesAsyncClient, final String arn) {
        this.snsAsyncClient = sesAsyncClient;
        this.topicArn = arn;
    }

    @Override
    public CompletionStage<String> onNext(final String item) {
        LOG.trace("onNext invoked for topicARN:{} for item :{}", topicArn, item);
        return snsAsyncClient.publish(PublishRequest.builder().topicArn(topicArn).message(item).build())
                .thenApply(Objects::toString);
    }
}

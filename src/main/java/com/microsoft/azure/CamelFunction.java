package com.microsoft.azure;

import static org.apache.camel.component.kafka.KafkaConstants.KEY;
import static org.apache.camel.component.kafka.KafkaConstants.PARTITION_KEY;
import java.util.Optional;
import java.util.Random;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.ExchangeBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConfiguration;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultProducerTemplate;

/**
 * A Camel Application
 */
public class CamelFunction {

    private CamelContext camelContext;
    private ProducerTemplate producer;

    public CamelFunction() throws Exception {
        System.out.println("Starting camel...");

        // UPDATE WITH YOURS
        String kafkaEndpoint = ""; // <something>.servicebus.windows.net
        String sharedAccessKeyName = ""; // CHANGE ME
        String sharedAccessKey = ""; // CHANGE ME

        KafkaConfiguration kafkaConfig = new KafkaConfiguration();
        kafkaConfig.setBrokers("brborgeseventhub.servicebus.windows.net:9093");
        kafkaConfig.setSaslJaasConfig("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"" +
                "Endpoint=sb://"+kafkaEndpoint+".servicebus.windows.net/;SharedAccessKeyName="+sharedAccessKeyName+";SharedAccessKey="+sharedAccessKey+";");
        kafkaConfig.setSaslMechanism("PLAIN");
        kafkaConfig.setSecurityProtocol("SASL_SSL");

        camelContext = new DefaultCamelContext();
        camelContext.getGlobalOptions().put("CamelJacksonEnableTypeConverter", "true");

        KafkaComponent kafkaComponent = camelContext.getComponent("kafka", KafkaComponent.class);
        kafkaComponent.setConfiguration(kafkaConfig);

        camelContext.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:toUpperCase")
                        .log("Message converted to UPPER case: ${body}")
                        .process(e -> e.getOut().setBody(e.getIn().getBody().toString().toUpperCase()));

                from("direct:message-splitter")
                        .convertBodyTo(String.class)
                        .choice()
                            .when(header("Content-Type").isEqualTo("application/json"))
                                .to("direct:json-splitter")
                            .when(header("Content-Type").isEqualTo("application/xml"))
                                .to("direct:xml-splitter")
                            .when(header("Content-Type").isEqualTo("plain/text"))
                                .to("direct:text-splitter")
                            //.when(header("Content-Type").isEqualTo("text/csv"))
                            //    .to("direct:csv-splitter")
                            .otherwise()
                                .to("direct:no-splitter")
                        .end();

                from("direct:dispatch")
                        .log("Dispatching the following message: ${body}")
                        .setHeader(Exchange.HTTP_URI, header("Destination").getExpression())
                        .setHeader(Exchange.HTTP_METHOD, header("Destination-Method").getExpression())
                        .to("http4://mock.address");

                from("direct:json-splitter")
                        .log("Split by JSON Array Element")
                        //.marshal().json()
                        .log("${body}")
                        .split()
                            .jsonpathWriteAsString("${header.Split-Pattern}", false)
                            .streaming()
                            .to("direct:dispatch");

                from("direct:xml-splitter")
                        .log("Split by XML Element")
                        .split()
                            .xpath("${header.Split-Pattern}")
                        .log("${body}")
                        .to("direct:dispatch");

                from("direct:text-splitter")
                        .log("Split by plain text regular expression")
                        .split()
                            .simple("${header.Split-Pattern}")
                        .to("direct:dispatch");

                /*
                from("direct:csv-splitter")
                        .log("Split by CSV line")
                        .split(body().tokenize("\n"))
                        .end();
                */

                from("direct:no-splitter")
                        .to("direct:dispatch");

                /*
                from("direct:message-filter")
                        .filter();
                 */

                from("direct:queueOnKafka")
                        .log("${body}")
                        .convertBodyTo(String.class)
                        .process(exchange -> {
                            exchange.getIn().setHeader(PARTITION_KEY, 0);
                            exchange.getIn().setHeader(KEY, Integer.toString(new Random().nextInt(100)));
                        }).to("kafka:messages");

            }
        });
        camelContext.start();

        producer = new DefaultProducerTemplate(camelContext);
        producer.start();
    }

    @FunctionName("upper")
    public HttpResponseMessage upper(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) throws Exception {

        String body = request.getBody().orElse(request.getQueryParameters().get("body"));
        Exchange exchange = ExchangeBuilder.anExchange(camelContext).withBody(body).build();
        Exchange out = producer.send("direct:toUpperCase", exchange);
        String outMessage = out.getOut().getBody().toString();
        System.out.println("Message converted: " + outMessage);

        return request.createResponseBuilder(HttpStatus.OK).body("Camel returned: " + outMessage).build();
    }

    @FunctionName("queueOnKafka")
    public HttpResponseMessage queueOnKafka(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) throws Exception {

        String body = request.getBody().orElse("MISSING VALUE");
        Exchange exchange = ExchangeBuilder.anExchange(camelContext).withBody(body).build();
        Exchange out = producer.send("direct:queueOnKafka", exchange);

        return request.createResponseBuilder(HttpStatus.OK).body("Camel returned: " + out.getOut().getBody()).build();
    }

    @FunctionName("messageSplitter")
    public HttpResponseMessage messageSplitter(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) throws Exception {

        String body = request.getBody().orElse(request.getQueryParameters().get("body"));
        Exchange exchange = ExchangeBuilder
                .anExchange(camelContext)
                .withBody(body)
                .build();

        request.getHeaders().forEach((k, v) -> exchange.getIn().setHeader(k, v));

        producer.asyncSend("direct:message-splitter", exchange);

        return request.createResponseBuilder(HttpStatus.OK).body("Message received. Check logs.").build();
    }

    @FunctionName("messageFilter")
    public HttpResponseMessage messageFilter(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) throws Exception {

        String body = request.getBody().orElse(request.getQueryParameters().get("body"));
        Exchange exchange = ExchangeBuilder
                .anExchange(camelContext)
                .withBody(body)
                .withHeader("filter-pattern", request.getHeaders().get("filter-pattern"))
                .build();

        Exchange out = producer.send("direct:message-filter", exchange);

        return request.createResponseBuilder(HttpStatus.OK).body(out.getOut().getBody(String.class)).build();
    }
}

FROM spark-disjoint-ranges as build

FROM openjdk:8-jre-alpine
COPY --from=build \
   /app/generator/target/scala-2.12/generator-assembly-0.1.0-SNAPSHOT.jar .

CMD java -jar /generator-assembly-0.1.0-SNAPSHOT.jar
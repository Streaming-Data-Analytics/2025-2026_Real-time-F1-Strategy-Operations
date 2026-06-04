FROM maven:3.9-eclipse-temurin-17

WORKDIR /work

COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline

COPY . .
RUN chmod +x run.sh

ENTRYPOINT ["./run.sh"]
CMD ["all"]

package pl.edu.agh.ki.bd2;

import org.neo4j.graphdb.*;

public class Solution {

    private final GraphDatabase graphDatabase = GraphDatabase.createDatabase();

    public void databaseStatistics() {
        System.out.println(graphDatabase.runCypher("CALL db.labels()"));
        System.out.println(graphDatabase.runCypher("CALL db.relationshipTypes()"));
    }

    public void runAllTests() {
        System.out.println(findActorByName("Emma Watson"));
        System.out.println(findMovieByTitleLike("Star Wars"));
        System.out.println(findRatedMoviesForUser("maheshksp"));
        System.out.println(findCommonMoviesForActors("Emma Watson", "Daniel Radcliffe"));
        System.out.println(findMovieRecommendationForUser("adilfulara"));
        createNewNodes("Bardzo-dobry Aktor", "Polska Komedia 38", "Aktor");
        System.out.println(setPropertiesOfNewNode("Bardzo-dobry Aktor", "11-12-1986", "Kraków"));
        System.out.println(findActorByName("Bardzo-dobry Aktor"));
        System.out.println(actorsWhoActedInAtLeast6Films());
        System.out.println(avgMovieAppearancesForActorsWhoActedInAtLeast7Movies());
        System.out.println(actorsWhoActedInAtLeast5MoviesAndDirectedAtLeastOne());
        System.out.println(usersFriendWhoVotedMovieForAtLeast3Stars("adilfulara"));
        System.out.println(pathBeetween2Actors("Kevin Bacon", "Tomasz Karolak"));
        checkTimeOfActorSearchWithAndWithoutIndex("Emma Watson");
    }

    private String findActorByName(final String actorName) {
        return graphDatabase.runCypher("MATCH (a:Actor {name: '" + actorName + "'}) RETURN a");
    }

    private String findMovieByTitleLike(final String movieName) {
        return graphDatabase.runCypher("MATCH (m:Movie) WHERE m.title CONTAINS '" + movieName + "' RETURN m.title");
    }

    private String findRatedMoviesForUser(final String userLogin) {
        return graphDatabase.runCypher("MATCH (u:User {login: '" + userLogin + "'}) -[:RATED]-> (m:Movie) RETURN m.title");
    }

    private String findCommonMoviesForActors(String actorOne, String actrorTwo) {
        return graphDatabase.runCypher("MATCH (a:Actor {name: '" + actorOne + "'}) -[:ACTS_IN]-> (m:Movie) <-[:ACTS_IN]- " +
                "(a2:Actor {name: '" + actrorTwo + "'}) return m.title");
    }

    private String findMovieRecommendationForUser(final String userLogin) {
        return graphDatabase.runCypher("MATCH (u1:User {login: '" + userLogin + "'}) -[:FRIEND]-> (u2:User) -[r:RATED]-> " +
                "(m:Movie) WHERE r.stars >= 4 RETURN u2.name, m.title, r.stars");
    }

    //zadanie 4.
    private void createNewNodes(String actorName, String movieName, String role) {
        GraphDatabaseService service = graphDatabase.getGraphDatabaseService();
        try ( Transaction tx = service.beginTx() )
        {
            Label actor = null;
            Label movie = null;
            RelationshipType relationshipType = null;

            for (Label l: service.getAllLabels()) {
                if (l.name().equals("Movie")) movie = l;
                if (l.name().equals("Actor")) actor = l;
            }
            Node actorNode = service.createNode(actor);
            Node movieNode = service.createNode(movie);
            actorNode.setProperty("name", actorName);
            movieNode.setProperty("title", movieName);

            for (RelationshipType r: service.getAllRelationshipTypes()) {
                if (r.name().equals("ACTS_IN")) relationshipType = r;
            }
            Relationship relationship = actorNode.createRelationshipTo(movieNode, relationshipType);
            relationship.setProperty("name", role);

            tx.success();
        }
    }

    //zadanie 5.
    private String setPropertiesOfNewNode(String actorName, String birthdate, String birthplace) {
        return graphDatabase.runCypher("MATCH (a:Actor {name: '" + actorName + "'}) set a.birthdate = '" + birthdate +
                "' set a.birtplace = '" + birthplace + "'");
    }

    //zadanie 6.
    private String actorsWhoActedInAtLeast6Films() {
        return graphDatabase.runCypher("MATCH (a:Actor) -[:ACTS_IN]-> (m) WITH length(collect({title: m.title})) as " +
                "numOfMovies, a WHERE numOfMovies >= 6 RETURN a.name, numOfMovies");
    }

    //zadanie 7.
    private String avgMovieAppearancesForActorsWhoActedInAtLeast7Movies() {
        return graphDatabase.runCypher("MATCH (a:Actor) -[:ACTS_IN]-> (m) WITH length(collect({title: m.title})) as " +
                "numOfMovies, a WHERE numOfMovies >= 7 RETURN avg(numOfMovies)");
    }

    //zadanie 8.
    private String actorsWhoActedInAtLeast5MoviesAndDirectedAtLeastOne() {
        return graphDatabase.runCypher("MATCH (a:Actor) -[:ACTS_IN]-> (m:Movie) WITH a, count(m) as movies MATCH " +
                "(a:Director) -[:DIRECTED]-> (m:Movie) WITH a, movies, count(m.title) as directed WHERE movies >= 5 AND " +
                "directed >= 1 RETURN a.name, movies, directed ORDER BY movies DESC");
    }

    //zadanie 9.
    private String usersFriendWhoVotedMovieForAtLeast3Stars(String userLogin) {
        return graphDatabase.runCypher("MATCH (u:User {login: '" + userLogin + "'}) -[:FRIEND]-> (u1:User) -[r:RATED]->" +
                "(m:Movie) WHERE r.stars >= 3 RETURN u1.login, u1.name, m.title, r.stars");
    }

    //zadanie 10.
    private String pathBeetween2Actors(String actorOne, String actorTwo) {
        return graphDatabase.runCypher("MATCH p=shortestPath((a:Actor {name: '" + actorOne + "'})-[*]-(a1:Actor {name: '" +
                actorTwo + "'})) return extract(n in filter(x in nodes(p) where (x:Actor)) | n.name) as path");
    }

    //zadanie 11.
    private void checkTimeOfActorSearchWithAndWithoutIndex(String actorName) {
        graphDatabase.runCypher("CREATE INDEX ON :Actor(name)");
        System.out.println("----------------SEARCHING WITH INDEX CREATED-------------");
//        System.out.println(graphDatabase.runCypher("EXPLAIN MATCH (a:Actor {name: '" + actorName + "'}) RETURN a"));
        System.out.println(graphDatabase.runCypher("PROFILE MATCH (a:Actor {name: '" + actorName + "'}) RETURN a"));
        graphDatabase.runCypher("DROP INDEX ON :Actor(name)");
        System.out.println("----------------SEARCHING WITHOUT INDEX CREATED----------");
//        System.out.println(graphDatabase.runCypher("EXPLAIN MATCH (a:Actor {name: '" + actorName + "'}) RETURN a"));
        System.out.println(graphDatabase.runCypher("PROFILE MATCH (a:Actor {name: '" + actorName + "'}) RETURN a"));
    }
}

import sqlite3
from relational_algebra import *
class Expressions:
    # You can manually define expressions here or load from the uploaded file
    # Example Query: Find Databases conference and their rankings
    sample_query = Projection(
            ThetaJoin(
                Selection(
                    Relation("field_conference"),
                    Equals("field", "Databases")
                ),
                Relation("conference_ranking"),
                Equals("field_conference.conference", "conference_ranking.conf_abbr")
            ),
            ["conference_ranking.conf_abbr", "conference_ranking.rank"]
        )
    
    expression1 =Projection(
                    NaturalJoin(
                        Projection(
                            Selection(
                                Relation("field_conference"), 
                                Equals("field", "Databases")
                                ),
                            ["field_conference.conference"]
                        ),
                        NaturalJoin(
                            Projection(
                                Selection(
                                    Relation("author"),
                                    Equals("author.affiliation","University of California, Berkeley")
                                ),
                                ["author.name"]
                            ),
                            Intersection(
                                Selection(
                                    Relation("pub_info",),
                                    GreaterEquals("pub_info.year",2010)
                                ),
                                Selection(
                                    Relation("pub_info",),
                                    LessEquals("pub_info.year",2024)
                                ),
                            )
                        )
                    ),
                    ["pub_info.name", "pub_info.conference","pub_info.year" ,"pub_info.count" ]
                )
                    

    expression2 = Projection( 
                        Selection(
                                Relation("field_conference"),
                                Equals("major","Computer Science")
                            ), 
                        ["field_conference.conference"]
                    ) - Projection(
                            NaturalJoin(
                                Relation("pub_info"),
                                Selection(
                                    Relation("author"),
                                    Equals("author.affiliation", "University of California, Irvine")
                                    )
                            ),
                            ["pub_info.conference"]
                        )
    
    # expression3 = ...
    
    # expression4 = ...
    
    # expression5 = ...

    # Example query execution


sql_con = sqlite3.connect('sample220P.db')  # Ensure the uploaded database is loaded here
result = Expressions.expression1.evaluate(sql_con=sql_con)
print(result.rows)
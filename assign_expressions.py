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
    
    
    #Q1
    # πname,conference,year,count((σfield='Databases'(field_conference) ⨝ σ2010≤year∧year≤2024(pub_info)) ⨝ σaffiliation='UniversityofCalifornia,Berkeley'(author))
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
                    
    # Q2
    # πconference(σfield_conference.major='Computer Science'(field_conference)) - πconference(σauthor.affiliation='University of California, Irvine'(author⨝pub_info))
    exp2_1 = Projection( 
                        Selection(
                                Relation("field_conference"),
                                Equals("major","Computer Science")
                            ), 
                        ["field_conference.conference"]
                    )
    exp2_2 = Projection(
                            NaturalJoin(
                                Relation("pub_info"),
                                Selection(
                                    Relation("author"),
                                    Equals("author.affiliation", "University of California, Irvine")
                                    )
                            ),
                            ["pub_info.conference"]
                        )
    expression2 = x - y

    # Q3
    # πauthor.name,author.affiliation,pub_info.conference,pub_info.year,count(author⨝(pub_info⨝(γfield_conference.conference,pub_info.year;max(pub_info.count)→count(πconference(σfield='Databases'(field_conference))⨝pub_info))))
    # expression3 = ...
    
    # expression4 = ...
    
    # expression5 = ...

    # Example query execution


# Q4
# X = author ⨝ ( 
# σfield='Databases'(field_conference) ⨝ σyear≥2020 ∧ year≤2024(pub_info) )


# πaffiliation( X ) 
 
#  -
 
# πaffiliation(
# πaffiliation( X ) ⨯ πconference(σfield='Databases'(field_conference) ) - πaffiliation,conference( X ) )

# Q5
# X = πfield_conference.conference(σ(conference_ranking.conference_abbreviation=field_conference.conference)(σfield='Databases'(field_conference)⨝σrank='A*'(conference_ranking)))

# Y= πUniRank1.university_name,UniRank1.rank(ρUniRank1(usnews_university_rankings) ⨝ UniRank1.rank < usnews_university_rankings.rank σuniversity_name = 'UniversityofCalifornia,Irvine' (usnews_university_rankings))

# Z =πpub_info.name,author.affiliation(πpub_info.name(pub_info ⨝ X)⨝author)

# πauthor.affiliation(Y ⨝(UniRank1.university_name=author.affiliation) Z)


sql_con = sqlite3.connect('sample220P.db')  # Ensure the uploaded database is loaded here
result = Expressions.expression2.evaluate(sql_con=sql_con)
print(result.rows)

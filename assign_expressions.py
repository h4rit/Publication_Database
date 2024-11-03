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
    expression2 = exp2_1 - exp2_2

    # Q3
#     X = σfield='Databases'(field_conference)

# Y = X ⨝ pub_info
# Y1 = Y
# Y2 = ρY2(Y)

# Z = πfield_conference.conference,pub_info.year,pub_info.count,pub_info.name(Y1⨯Y2) - 
# πfield_conference.conference,pub_info.year,pub_info.count,pub_info.name(σ((field_conference.conference	=Y2.conference)∧(pub_info.year=Y2.year)∧(pub_info.count<Y2.count))(Y1⨯Y2))

# W = Z ⨝ Y
# R = πfield_conference.conference,pub_info.year,pub_info.name,author.affiliation(Z ⨝(pub_info.name=author.name) author)
# R
    expression3_1 = Selection(
                      Relation("field_conference"),
                      Equals("field_conference.field","Databases")
                    )
    expression3_2 = NaturalJoin(
                      expression3_1,
                      Relation("pub_info")
                    )
    expression3_Y2 = Rename(
                      expression3_2,
                      {
                          "field_conference.conference": "Y2_conference", # Changed 'Y2.conference' to 'Y2_conference'
                          "pub_info.year": "Y2_year", # Changed 'Y2.year' to 'Y2_year'
                          "pub_info.count": "Y2_count", # Changed 'Y2.count' to 'Y2_count'
                          "pub_info.name": "Y2_name" # Changed 'Y2.name' to 'Y2_name'
                      }
                    )
    expression3_3 = Projection(
                      Selection(
                        expression3_2 * expression3_Y2,
                        And(
                          Equals("field_conference.conference", "Y2_conference"), # Changed 'Y2.conference' to 'Y2_conference'
                          And(
                              Equals("pub_info.year", "Y2_year"), # Changed 'Y2.year' to 'Y2_year'
                              GreaterEquals("pub_info.count", "Y2_count") # Changed 'Y2.count' to 'Y2_count'
                          )
                        )
                      ),
                      ["field_conference.conference", "pub_info.year", "pub_info.count", "pub_info.name"]
                    )
    expression3_4 = Projection(
                      Selection(
                        expression3_2*expression3_Y2,
                        And(
                          Equals("field_conference.conference", "Y2_conference"),
                          And(
                            Equals("pub_info.year", "Y2_year"),
                            LessThan("pub_info.count", "Y2_count")
                          )
                        )
                      ),
                    ["field_conference.conference", "pub_info.year", "pub_info.count", "pub_info.name"]
                  )
    expression3_Z = expression3_3-expression3_4

    expression3_W = NaturalJoin(
                      expression3_Z,
                      expression3_2
                    )
    expression3 = Projection(NaturalJoin(
                      expression3_W,
                      Relation("author")
                    ),["field_conference.conference","pub_info.year","pub_info.name","author.affiliation"])
    

    # X = author ⨝ ( 
    # σfield='Databases'(field_conference) ⨝ σyear≥2020 ∧ year≤2024(pub_info) )
    # πaffiliation( X ) 
 
    #  -
    
    # πaffiliation(
    # πaffiliation( X ) ⨯ πconference(σfield='Databases'(field_conference) ) - πaffiliation,conference( X ) )

    expression4_1 = NaturalJoin(
                    Relation("author"),
                        NaturalJoin(
                            Selection(
                                Relation("pub_info"),
                                And(
                                    GreaterEquals("pub_info.year", 2020),
                                    LessEquals("pub_info.year", 2024)
                                )
                            ),
                            Selection( 
                                Relation("field_conference"),
                                Equals("field_conference.field", "Databases")
                            )
                        )
                    )
    # πaffiliation( X ) ⨯ πconference(σfield='Databases'(field_conference) ) - πaffiliation,conference( X ) )
    expression4 = Projection(expression4_1, ["author.affiliation"]) - Projection(
                    CrossProduct(
                        Projection (expression4_1,  ["author.affiliation"]),
                        Projection(
                            Selection(
                                Relation("field_conference"),
                                Equals('field_conference.field', "Databases")
                            ),
                            ['field_conference.conference']
                        )            
                    ) - Projection(expression4_1, ['author.affiliation','field_conference.conference']),
                    ['author.affiliation']

                )
    
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
# X= πfield_conference.conference(σ(conference_ranking.conference_abbreviation=field_conference.conference)(σfield='Databases'(field_conference)⨝σrank='A*'(conference_ranking)))

# Y= πUniRank1.university_name,UniRank1.rank(ρUniRank1(usnews_university_rankings) ⨝ UniRank1.rank < usnews_university_rankings.rank σuniversity_name = 'UniversityofCalifornia,Irvine' (usnews_university_rankings))

# Z= πpub_info.name,author.affiliation(πpub_info.name(pub_info ⨝ X)⨝author)

# exp5_1 = πauthor.affiliation(Y ⨝(UniRank1.university_name=author.affiliation) Z)

# YY = πUniRank1.university_name(Y)

# exp5 = YY - exp5_1

    X = Projection(
            ThetaJoin(
                Selection(
                    Relation("field_conference"),
                    Equals("field_conference.field", "Databases")
                ),
                Selection(
                    Relation("conference_ranking"),
                    Equals("conference_ranking.rank", "A*")
                ),
            Equals("conference_ranking.conf_abbr", "field_conference.conference")
            ),
        ["conference"]
    )


    UniRank1 = Rename(
        Relation("usnews_university_rankings"), "UniRank1"
    )

    Y = Projection(
            ThetaJoin(
                UniRank1,
                Selection(
                    Relation("usnews_university_rankings"),
                    Equals("university_name", "University of California, Irvine")
                ),
                LessThan("UniRank1.rank", "usnews_university_rankings.rank")
            ),
        ["UniRank1.university_name", "UniRank1.rank"]
    )

    YY = Projection ( Y, ['university_name'])

    Z = Projection(
            NaturalJoin(
                Projection(
                    NaturalJoin(Relation("pub_info"), X),
                    ["pub_info.name"]
                ),
                Relation("author")
            ),
        ["pub_info.name", "author.affiliation"]
    )

    expression5_1 = Projection(
        ThetaJoin(Y,Z,Equals("UniRank1.university_name", "author.affiliation")
        ),
        ["affiliation"]
    ) 
    # expression5_1 = Rename(expression5_1, mapping={"affiliation": "affiliation"})
    YY = Rename(YY, mapping={"university_name": "affiliation"})
    # expression5 = YY - RightSemiJoin(YY, expression5_1)
    expression5 = YY - expression5_1


sql_con = sqlite3.connect('sample220P.db')  # Ensure the uploaded database is loaded here


# result = Expressions.YY.evaluate(sql_con=sql_con)
# print(result.rows)
# result = Expressions.expression5_1.evaluate(sql_con=sql_con)
# print(result.rows)
result = Expressions.expression3.evaluate(sql_con=sql_con)
print(result.rows)
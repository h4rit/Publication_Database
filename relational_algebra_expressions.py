from relational_algebra import *
import sqlite3
class Expressions():
    
    expression1 = Projection(Relation("person"), ["name", "gender"])

    expression2 = Projection(
        Selection(
            NaturalJoin(
                NaturalJoin(
                    Relation("university_affiliate"),
                    Relation("person")
                ),Relation("non_student")
            ), Equals("non_student.member_type", "Faculty")
        ), ["person.name", "university_affiliate.department"]
    )

    expression3 = Projection(
        Selection(
            ThetaJoin(
                NaturalJoin(
                    NaturalJoin(
                        Relation("location_reading"),
                        Relation("location_sensor")
                    ),
                    Selection(
                        Relation("space"),
                        Or(
                            Equals("space_description", "weight room"), 
                            Equals("space_description", "cardio room")
                        )
                    )
                ),
                Relation("person"),
                Equals("location_reading.person_id", "person.card_id")
            ),
            And(
                GreaterEquals("timestamp", "2023-04-01 00:00:00"),
                LessThan("timestamp", "2023-04-02 00:00:00")
            )
        ),
        ["name"]
    )

    expression4 = Projection(
        NaturalJoin(
            Projection(
                Relation("attends"), ["card_id", "event_id"]
            ) / Projection(Relation("attends"), ["event_id"]),
            Relation("person")
        ), 
        ["person.name"]
    ) 

    expression5 = Projection(
        Selection(
            NaturalJoin(Relation("events"), Relation("space")),
            GreaterEquals("events.capacity", "space.max_capacity")
        ), ["events.event_id"])

    expression6 = Projection(
            NaturalJoin(
                (Projection(
                Selection(
                    NaturalJoin(
                        NaturalJoin(
                            NaturalJoin(
                                Relation("usage_reading"),
                                Relation("equipment")
                            ),
                            Relation("space")
                        ),
                        Relation("student")
                    ), Equals("space.space_description", "cardio room")
                ), ["student.card_id", "equipment.equipment_id"]
            ) / Projection(
                    Selection(
                        NaturalJoin(
                            Relation("equipment"),
                            Relation("space")
                        ),  Equals("space.space_description", "cardio room")
                    ), ["equipment.equipment_id"]
                )
            ),
            Relation("person")
        ), ["person.name"]
    )

    expression7 = Projection(
        Selection(
            Relation("equipment"),
            Equals("is_available", 1)
        ), ["equipment_id", "equipment_type"]
    )

    expression8 = Projection(
        NaturalJoin(
            Relation("employee"),
            Relation("person")
        ), ["name"]
    )

    expression9 = Projection(
        NaturalJoin(
            NaturalJoin(
                Selection(
                    NaturalJoin(
                        NaturalJoin(
                            Relation("attends"),
                            Relation("events")
                        ),
                        Relation("space")
                    ),
                    Equals("space_description", "yoga studio")
                ),
                Relation("member")
            ),
            Relation("person")
        ),
        ["name"]
    )

    expression10 = Projection(
        NaturalJoin(
            NaturalJoin(
                Selection(
                    NaturalJoin(
                        Relation("attends"),
                        Relation("events")
                    ),
                    Equals("description", "Summer Splash Fest")
                ),
                Relation("family")
            ),
            Relation("person")
        ),
        ["name"]
    )
    
    
    # sql_con = sqlite3.connect("sample220P.db")

    # result = expression3.evaluate(sql_con=sql_con)
    # print(result.rows, len(result.rows))


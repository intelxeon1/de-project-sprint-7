from enum import Enum

r = 6371
rec_dist = 1
cities_tz = [
    (1, "Australia/Sydney"),
    (2, "Australia/Melbourne"),
    (3, "Australia/Brisbane"),
    (4, "Australia/Perth"),
    (5, "Australia/Adelaide"),
    (6, "Australia/Brisbane"),
    (7, "Australia/Melbourne"),
    (8, "Australia/Sydney"),
    (9, "Australia/Sydney"),
    (10, "Australia/Sydney"),
    (11, "Australia/Melbourne"),
    (12, "Australia/Hobart"),
    (13, "Australia/Brisbane"),
    (14, "Australia/Brisbane"),
    (15, "Australia/Brisbane"),
    (16, "Australia/Brisbane"),
    (17, "Australia/Darwin"),
    (18, "Australia/Melbourne"),
    (19, "Australia/Melbourne"),
    (20, "Australia/Hobart"),
    (21, "Australia/Brisbane"),
    (22, "Australia/Brisbane"),
    (23, "Australia/Sydney"),
    (24, "Australia/Perth"),
]
home_depth = 27    
     
class Mart_Paths(Enum):
    usr = "users"
    zone = "zones"
    rec = "recomendation"
    

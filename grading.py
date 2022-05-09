import duckdb
import pandas as pd

lines = []

with open('project2.py','r') as f:
    for line in f.readlines():
        # avoid shell commands
        if line.startswith("!"):
            line = "# " + line
        lines.append(line)

with open('project2.py', 'w') as f:
    for line in lines:
        f.write(line)

import project2
        
db = duckdb.connect(':memory:')

def rungq(q):
    cursor = db.execute(q)
    df = cursor.fetchdf()
    return df

# use the small tweets dataset for grading
rungq("""
CREATE OR REPLACE TABLE tweets(idx INTEGER, create_time VARCHAR, id DOUBLE, in_reply VARCHAR, like_num INTEGER, quoted_org_id DOUBLE, retweet_num INTEGER, retweet_org_id VARCHAR, \"text\" VARCHAR, twitter_username VARCHAR, twitch_username VARCHAR);
COPY tweets FROM \'tweets.csv\' (FORMAT \'csv\', quote \'\"\', header 0, delimiter \',\');
""")

class TestValue():
    def assertEqual(self, v1, v2, msg):
        if v1 != v2:
            print(msg)
            
    def setUp(self):
        self.q1 = project2.q1
        self.q2 = project2.q2
        self.q3 = project2.q3
        self.q4 = project2.q4
        self.q5 = project2.q5
        self.q6 = project2.q6
        self.q8 = project2.q8
        self.page_rank = project2.page_rank

    def test_q1(self):
        result = rungq(self.q1)
        self.assertEqual(len(result.columns), 2, "result columns are wrong")
        self.assertEqual(len(result), 3, "result rows are wrong")
        self.assertEqual(int(result["id"].iloc[0]), 979101545676595072, "result id is wrong")
        
    def test_q2(self):
        result = rungq(self.q2)
        self.assertEqual(len(result.columns), 3, "result columns are wrong")
        self.assertEqual(len(result), 5, "result rows are wrong")
        self.assertEqual(2018 in list(pd.to_numeric(result["year"],errors='coerce')), True, "result year is wrong")
        self.assertEqual(9 in list(pd.to_numeric(result["month"],errors='coerce')), True, "result month is wrong")
        self.assertEqual(13 in list(pd.to_numeric(result["count"],errors='coerce')), True, "result count is wrong")
        
    def test_q3(self):
        rungq(self.q3)
        test_graph = "SELECT * FROM Graph;"
        result = rungq(test_graph)
        self.assertEqual(len(result.columns), 2, "result columns are wrong")
        self.assertEqual(len(result), 28673, "result rows are wrong")
        self.assertEqual("crkie" in list(result["src"]), True, "result src is wrong")
        self.assertEqual("awscloud" in list(result["src"]), True, "result src is wrong")
        self.assertEqual("CMDRZman" in list(result["src"]), True, "result src is wrong")
        self.assertEqual("lomadia" in list(result["dst"]), True, "result dst is wrong")
        self.assertEqual("cyberiantiger66" in list(result["dst"]), True, "result dst is wrong")
        self.assertEqual("cellbit" in list(result["dst"]), True, "result dst is wrong")
        
    def test_q4(self):
        # provide another graph
        create_graph = """DROP TABLE IF EXISTS Graph;
CREATE TABLE IF NOT EXISTS Graph AS 
with tmp as (select twitter_username as src, regexp_extract(text, '@[\w\d]+') as dst from tweets)
select distinct src, dst[1:] as dst from tmp where prefix(dst, '@');"""
        rungq(create_graph)
        result = rungq(self.q4)
        self.assertEqual(len(result.columns), 2, "result columns are wrong")
        self.assertEqual(len(result), 1, "result rows are wrong")
        self.assertEqual(result["max_indegree"].iloc[0], 'YouTube', "result max_indegree is wrong")
        self.assertEqual(result["max_outdegree"].iloc[0], 'Nadeshot', "result max_outdegree is wrong")
        
    def test_q5(self):
        result = rungq(self.q5)
        self.assertEqual(len(result.columns), 1, "result columns are wrong")
        self.assertEqual(len(result), 1, "result rows are wrong")
        self.assertEqual(abs(result["unpopular_popular"].iloc[0] - 0.001781) < 0.00001, True, "result unpopular_popular is wrong")
        
    
    def test_q6(self):
        result = rungq(self.q6)
        self.assertEqual(len(result.columns), 1, "result columns are wrong")
        self.assertEqual(len(result), 1, "result rows are wrong")
        # both 300 or 100 (divide by 3) are acceptable
        self.assertEqual(abs(result["no_of_triangles"].iloc[0] - 300) < 0.000001 or abs(result["no_of_triangles"].iloc[0] - 100) < 0.000001, True, "result no_of_triangles is wrong")

    def test_q7(self):
        result = self.page_rank(20, db)
        self.assertEqual(len(result.columns), 2, "result columns are wrong")
        self.assertEqual("KillerEmcee" in list(result["username"]), True, "result username is wrong")
        self.assertEqual("AlexanderCavali" in list(result["username"]), True, "result username is wrong")
        self.assertEqual("Bellla391" in list(result["username"]), True, "result username is wrong")
        self.assertEqual(abs(result["page_rank_score"][result["username"] == "KillerEmcee"].iloc[0] - 4.5471e-05) < 1e-07, True, "page_rank_score is wrong")
        self.assertEqual(abs(result["page_rank_score"][result["username"] == "AlexanderCavali"].iloc[0] - 4.5471e-05) < 1e-07, True, "page_rank_score is wrong")
        self.assertEqual(abs(result["page_rank_score"][result["username"] == "Bellla391"].iloc[0] -4.440535194616224e-08) < 1e-12, True, "page_rank_score is wrong")
        
    
    def test_q8(self):
        result = rungq(self.q8)
        self.assertEqual(len(result.columns), 2, "result columns are wrong")
        self.assertEqual("KillerEmcee" in list(result["username"]), True, "result username is wrong")
        self.assertEqual("AlexanderCavali" in list(result["username"]), True, "result username is wrong")
        self.assertEqual("Bellla391" in list(result["username"]), True, "result username is wrong")
        self.assertEqual(abs(result["page_rank_score"][result["username"] == "KillerEmcee"].iloc[0] - 4.5471e-05) < 1e-07, True, "page_rank_score is wrong")
        self.assertEqual(abs(result["page_rank_score"][result["username"] == "AlexanderCavali"].iloc[0] - 4.5471e-05) < 1e-07, True, "page_rank_score is wrong")
        self.assertEqual(abs(result["page_rank_score"][result["username"] == "Bellla391"].iloc[0] -4.440535194616224e-08) < 1e-12, True, "page_rank_score is wrong")
        
t = TestValue()
t.setUp()
print("------q1------")
t.test_q1()
print("------q2------")
t.test_q2()
print("------q3------")
t.test_q3()
print("------q4------")
t.test_q4()
print("------q5------")
t.test_q5()
print("------q6------")
t.test_q6()
print("------q7------")
t.test_q7()
print("------q8------")
t.test_q8()
print("------end------")
# season_position_forecast.py  –  CLEAN VERSION, NO LEAKAGE
from __future__ import annotations
import sqlite3, warnings
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import classification_report

DB_PATH   = "merged_all_seasons.db"
TABLE     = "all_merged_data"
TRAIN_YEARS = ["2021/22", "2022/23", "2023/24"]
TEST_YEAR   = "2024/25"

_SYNON = {
    "score": ["score","goals"],
    "xg":    ["xg"],
    "possession": ["possession","poss"],
    "corners":    ["corners","corner"],
    "yellow_cards": ["yellow_cards","yellows"],
}
SEASON_SHIFT = {"2021/22":"2022/23","2022/23":"2023/24",
                "2023/24":"2024/25","2024/25":"2025/26"}

# ───────── helpers ─────────
def _pct(s): return s.astype(str).str.rstrip("%").replace({"nan":np.nan}).astype(float)
def _find(df, base, side):
    for syn in _SYNON[base]:
        for cand in (f"{syn}_{side}", f"{side}_{syn}"):
            if cand in df.columns: return cand
    raise KeyError(base, side)

# ───────── load & clean ─────────
def load_matches():
    with sqlite3.connect(DB_PATH) as con:
        df = pd.read_sql(f"SELECT *, rowid rid FROM {TABLE} ORDER BY rid", con)
    df["_idx"] = (df.id == 1).cumsum() - 1
    df["season"] = df["_idx"].map({0:"2024/25",1:"2023/24",2:"2022/23",3:"2021/22"})
    df.drop(columns=["rid","_idx"], inplace=True)
    for c in [c for c in df.columns if c.endswith("_home") and "%" in str(df[c].iloc[0])]:
        df[c] = _pct(df[c]); opp=c.replace("_home","_away")
        if opp in df.columns: df[opp] = _pct(df[opp])
    return df

# ───────── explode ─────────
def explode(df):
    bases=["score","xg","possession","corners","yellow_cards"]
    rows=[]
    for side in ("home","away"):
        opp="away" if side=="home" else "home"
        rec={"season":df.season,
             "team":df[f"{side}_team"],
             "is_home":int(side=="home"),
             "market_value_mil":df[f"{side}_market_value_total_mil_eur"],
             "avg_age":df[f"{side}_avg_age"]}
        for b in bases:
            rec[b]=df[_find(df,b,side)]
            rec[f"{b}_against"]=df[_find(df,b,opp)]
        rows.append(pd.DataFrame(rec))
    out=pd.concat(rows, ignore_index=True)
    out["match_no"]=out.groupby(["season","team"]).cumcount()+1
    return out

# ───────── preseason features ─────────
def build_preseason(tm):
    last3=tm.groupby(["season","team"])["match_no"].transform("max")-tm["match_no"]<3
    base=(tm[last3].groupby(["season","team"],as_index=False)
            .agg(market_value_mil=("market_value_mil","mean"),
                 avg_age=("avg_age","mean")))
    pts=(tm.groupby(["season","team"],as_index=False)
           .apply(lambda d:(d["score"]>d["score_against"]).sum()*3+
                            (d["score"]==d["score_against"]).sum(),
                  include_groups=False)
           .rename(columns={None:"points_total"}))
    pts["rank_prev"]=pts.groupby("season")["points_total"].rank("first",ascending=False).astype(int)
    pts["season"]=pts["season"].map(SEASON_SHIFT)
    pts["elo_start"]=2000-(pts["rank_prev"]-1)*25
    return base.merge(pts[["season","team","elo_start"]],on=["season","team"],how="left")

# ───────── true labels ─────────
def season_labels(tm):
    pts=(tm.groupby(["season","team"],as_index=False)
           .apply(lambda d:(d["score"]>d["score_against"]).sum()*3+
                            (d["score"]==d["score_against"]).sum(),
                  include_groups=False)
           .rename(columns={None:"points_total"}))
    pts["rank"]=pts.groupby("season")["points_total"].rank("first",ascending=False).astype(int)
    pts["target"]=np.select([pts["rank"]<=5,pts["rank"]>=18],[0,2],1)
    return pts[["season","team","target"]]

# ───────── rolling ─────────
def build_rolling(tm, up_to):
    m=tm[tm.match_no<=up_to].copy()
    m["points"]=3*(m.score>m.score_against).astype(int)+(m.score==m.score_against).astype(int)
    m["xg_diff"]=m["xg"]-m["xg_against"]
    def last5(s): return s.tail(5).sum()
    def last10(s): return s.tail(10).sum()
    agg=(m.groupby(["season","team"],as_index=False)
          .agg(form5=("points",last5),
               form10=("points",last10),
               xg_diff_mean=("xg_diff","mean")))
    agg=agg.merge(build_preseason(tm),on=["season","team"],how="left")
    agg=agg.merge(season_labels(tm), on=["season","team"], how="left")
    return agg

# ───────── model ─────────
def fit_cat(X,y):
    clf=CatBoostClassifier(iterations=600,depth=6,learning_rate=0.07,
                           l2_leaf_reg=6,loss_function="MultiClass",
                           random_state=42,verbose=False)
    clf.fit(X,y,cat_features=[X.columns.get_loc("team")])
    return clf

# ═════════ MAIN ═════════
warnings.filterwarnings("ignore")
raw=load_matches()
tm_all=explode(raw)

# -------- LOGS ---------
print("\n[LOG] Wiersze po explode – sezon : rows")
print(tm_all.groupby("season").size())
print("\n[LOG] Wiersze na drużynę (pierwsze 5):")
print(tm_all.groupby(["season","team"]).size().head())
print("\n[LOG] Pierwsze 3 rekordy:")
print(tm_all.head(3).to_string(index=False))
# -----------------------

# ----- PRE-SEASON -----
pre=build_preseason(tm_all).merge(season_labels(tm_all),on=["season","team"],how="left")
train=pre[pre.season.isin(TRAIN_YEARS)]
Xtr,ytr=train.drop(columns=["target","season"]),train["target"]
Xte,yte=pre[pre.season==TEST_YEAR].drop(columns=["target","season"]),pre[pre.season==TEST_YEAR]["target"]

mdl_pre=fit_cat(Xtr,ytr)
print("\n=== PRE-SEASON RAPORT (24/25) ===")
print(classification_report(yte, mdl_pre.predict(Xte), digits=3))

print("\nTOP-5 probabilities przed kolejką 1 (24/25):")
print(pd.DataFrame(mdl_pre.predict_proba(Xte),
                   columns=["P_TOP5","P_MID","P_RELEG"],
                   index=pre[pre.season==TEST_YEAR]["team"])
        .sort_values("P_TOP5",ascending=False).head(10))

# ----- ROLLING -----
print("\n=== ROLLING FORECAST walk-forward 24/25 ===")
f1s=[]
for rnd in (5,10,15,20,25,30):
    feats=build_rolling(tm_all, rnd)
    tr=feats[feats.season.isin(TRAIN_YEARS)]
    te=feats[feats.season==TEST_YEAR]
    Xtr,ytr=tr.drop(columns=["target","season"]),tr["target"]
    Xte,yte=te.drop(columns=["target","season"]),te["target"]
    mdl=fit_cat(Xtr,ytr)
    rep=classification_report(yte, mdl.predict(Xte), output_dict=True)
    f1=rep["weighted avg"]["f1-score"]; f1s.append(f1)
    print(f" • kolejka {rnd:2d}  F1 = {f1:.3f}")
print(f"\nŚrednie F1 = {np.mean(f1s):.3f}")

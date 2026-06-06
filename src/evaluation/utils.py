import math

import pandas as pd


def compare_pandas_table(pred: pd.DataFrame, gold: pd.DataFrame, condition_cols: list = [], ignore_order: bool = False, tolerance: float = 1e-2) -> int:    
    def vectors_match(v1, v2, tol=tolerance, ignore_order_=False):
        if len(v1) != len(v2):
            return False
        
        if ignore_order_:
            v1, v2 = (sorted(v1, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))),
                      sorted(v2, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))))
                      
        for a, b in zip(v1, v2):
            if pd.isna(a) and pd.isna(b):
                continue
            elif isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if not math.isclose(float(a), float(b), abs_tol=tol):
                    return False
            elif a != b:
                return False
        return True
    
    if condition_cols != []:
        gold_cols = gold.iloc[:, condition_cols]
    else:
        gold_cols = gold

    pred_cols = pred
    
    t_gold_list = gold_cols.transpose().values.tolist()
    t_pred_list = pred_cols.transpose().values.tolist()
    for _, gold in enumerate(t_gold_list):
        if not any(vectors_match(gold, pred, ignore_order_=ignore_order) for pred in t_pred_list):
            return 0

    return 1

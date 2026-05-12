
import os


final_path = "log_v3_topn100/sql_selection/final"
result_path = "log_v3_topn100/exec_results"
os.makedirs(result_path, exist_ok=True)

for instance in os.listdir(final_path):
    if os.path.exists(os.path.join(final_path, instance, 'result.csv')):
        # df = pd.read_csv(os.path.join(final_path, instance, 'result.csv'), encoding='utf-8')
        df = open(os.path.join(final_path, instance, 'result.csv'), 'r', encoding='utf-8').read()
        with open(os.path.join(result_path, f'{instance}.csv'), 'w', encoding='utf-8') as f:
            f.write(df)

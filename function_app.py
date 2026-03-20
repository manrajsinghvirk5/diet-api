import azure.functions as func
import pandas as pd
import json
import time

app = func.FunctionApp()

@app.route(route="analyze", auth_level=func.AuthLevel.ANONYMOUS)
def analyze(req: func.HttpRequest) -> func.HttpResponse:
    try:
        start_time = time.time()

        df = pd.read_csv('All_Diets.csv')

        diet = req.params.get('diet')

        if diet and diet.lower() != "all":
            df = df[df['Diet_type'].str.lower() == diet.lower()]

        if df.empty:
            return func.HttpResponse(
                json.dumps({
                    "labels": [],
                    "protein": [],
                    "carbs": [],
                    "fat": []
                }),
                mimetype="application/json"
            )

        avg_macros = df.groupby('Diet_type')[['Protein(g)', 'Carbs(g)', 'Fat(g)']].mean().round(2)

        result = {
            "labels": avg_macros.index.tolist(),
            "protein": avg_macros['Protein(g)'].tolist(),
            "carbs": avg_macros['Carbs(g)'].tolist(),
            "fat": avg_macros['Fat(g)'].tolist(),
            "executionTimeMs": int((time.time() - start_time) * 1000)
        }

        return func.HttpResponse(
            json.dumps(result),
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)

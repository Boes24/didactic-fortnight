from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTE
import pandas as pd
import Postgres
from Token_model import Token


def random_forest(xtrain, xtest, ytrain, ytest, use_smote=False):
    print("Random Forest training...")

    # Valgfrit SMOTE for at håndtere ubalancerede data
    if use_smote:
        smote = SMOTE(random_state=42)
        x_res, y_res = smote.fit_resample(xtrain, ytrain.values.ravel())
        print(f"SMOTE applied — samples before: {len(ytrain)}, after: {len(y_res)}")
    else:
        x_res, y_res = xtrain, ytrain.values.ravel()

    # Modelopsætning
    clf = RandomForestClassifier(
        random_state=42,
        n_jobs=8,
        class_weight='balanced_subsample',
        n_estimators=200,
        min_samples_leaf=3,
        min_samples_split=3
    )

    # Træning
    clf.fit(x_res, y_res)

    # Prediction
    y_pred = clf.predict(xtest)

    # Beregninger
    cm = confusion_matrix(ytest, y_pred)

    # Smukt formateret confusion matrix
    cm_df = pd.DataFrame(
        cm,
        index=['Actual: 0', 'Actual: 1'],
        columns=['Predicted: 0', 'Predicted: 1']
    )

    # Udskriv resultater
    print("\n* Confusion Matrix:")
    print(cm_df)

    print("\n* Classification Report:")
    print(classification_report(ytest, y_pred, digits=3))

    return clf

def train_ai_model(db: Postgres.Postgres, token:Token) -> RandomForestClassifier:
    print(f"train ai model for {token.name}")
    data = db.read_table_historic(token)
    data.drop("klineopentime", axis="columns", inplace=True)
    print(f"Data loaded")
    data = data.dropna()
    yvalues = pd.DataFrame(dict(goodbuytime=[]), dtype=int)
    yvalues["goodbuytime"] = data["goodbuytime"].copy()
    xvalues = data.drop("goodbuytime", axis="columns")

    # Generate train and test data sets
    xtrain, xtest, ytrain, ytest = train_test_split(xvalues, yvalues, test_size=0.2)
    token.model = random_forest(xtrain, xtest, ytrain, ytest)


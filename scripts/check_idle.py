"""Refuse to deploy while any DAG is queued or running."""
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session

with create_session() as session:
    active = session.query(DagRun).filter(DagRun.state.in_(["queued", "running"])).all()
    if active:
        raise SystemExit("Deployment deferred: active DAG runs: " + str([(r.dag_id, r.run_id) for r in active]))
print("No active DAG runs")

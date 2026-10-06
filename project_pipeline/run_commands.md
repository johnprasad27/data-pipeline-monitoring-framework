# Project Run and Stop Commands

This file contains the exact commands to start, stop, and test the Data Pipeline Monitoring and Recovery Framework.

---

## 1. Activate the virtual environment

PowerShell:

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
```

---

## 2. Start the scheduler / pipeline runner

From the project root:

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
python main.py
```

This starts the continuous pipeline scheduler.

To stop it:

```powershell
Ctrl + C
```

---

## 3. Start the Streamlit dashboard

From the project root in a second terminal:

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
streamlit run monitoring/dashboard.py
```

To stop it:

```powershell
Ctrl + C
```

---

## 4. Full startup sequence

Terminal 1:

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
python main.py
```

Terminal 2:

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
streamlit run monitoring/dashboard.py
```

---

## 5. Stop the whole system

Stop both foreground processes with:

```powershell
Ctrl + C
```

If you started the processes in separate terminals, stop each one individually.

---

## 6. Test success manually

Run a single API pipeline execution:

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
.\venv\Scripts\python.exe -c "from pipelines.api_pipeline import run_api_pipeline; print(run_api_pipeline())"
```

This triggers a pipeline run and records:
- RUNNING
- SUCCESS
- execution duration
- row count
- error details if any

---

## 7. Test failure and retry manually

Set an invalid API URL for the test run:

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
$env:API_BASE_URL = "http://127.0.0.1:1"
.\venv\Scripts\python.exe -c "from pipelines.api_pipeline import run_api_pipeline; print(run_api_pipeline())"
```

This should produce failure/retry behavior.

Then restore the real URL:

```powershell
$env:API_BASE_URL = "https://dummyjson.com/carts"
```

---

## 8. Run the CSV pipeline manually

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
.\venv\Scripts\python.exe -c "from pipelines.csv_pipeline import run_csv_pipeline; print(run_csv_pipeline('data/csv/sales.csv'))"
```

---

## 9. Run the project test validation

```powershell
cd "C:\Users\john27\MCA\mini_project\project_pipeline"
.\venv\Scripts\Activate.ps1
.\venv\Scripts\python.exe -m pytest tests/test_pipeline_events.py -q
```

---

## 10. Quick reference summary

Start scheduler:

```powershell
python main.py
```

Start dashboard:

```powershell
streamlit run monitoring/dashboard.py
```

Stop either process:

```powershell
Ctrl + C
```

Manual success test:

```powershell
.\venv\Scripts\python.exe -c "from pipelines.api_pipeline import run_api_pipeline; print(run_api_pipeline())"
```

Manual failure test:

```powershell
$env:API_BASE_URL = "http://127.0.0.1:1"
.\venv\Scripts\python.exe -c "from pipelines.api_pipeline import run_api_pipeline; print(run_api_pipeline())"
```

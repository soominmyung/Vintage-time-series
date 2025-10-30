## 📈 Vacancy Forecasting with Multi-Vintage Time Series

### 🧩 Overview  
This project demonstrates how to build a **robust time series forecasting pipeline** when the source data contain **multiple vintages** — repeated historical releases of the same series over time.  
It shows that with the right structuring and analysis, **simple statistical models** can achieve **high accuracy** even on revision-prone datasets.

---

### 🏗️ Project Structure  

| Step | Script | Description |
|------|---------|-------------|
| 1️⃣ **Data Fetching** | `fetch_vintages.py` | Automatically downloads multiple CSV vintages from a given public statistics portal. Skips already existing files for reproducibility. |
| 2️⃣ **Data Tidying** | `build_tidy.py` | Cleans and consolidates the vintages into a unified structure containing both the observation date and the publication (vintage) date. |
| 3️⃣ **Visualisation** | `Visualisation.html` | Interactive chart showing how estimates for each observation month evolve across successive vintages. |
| 4️⃣ **Forecasting** | `forecast.html` | Simple univariate forecasting model demonstrating strong performance due to the stable and well-structured nature of the series. |

```
├── src/
│   ├── fetch_vintages.py      # Automated vintage downloader
│   ├── build_tidy.py          # Consolidate vintages into tidy monthly dataset
├── analysis/
│   ├── visualisation.ipynb    # Revision patterns across vintages
│   └── forecast.ipynb         # Vintage-aware 1-step Holt–Winters forecast
├── data/
│   ├── raw/                   # Downloaded ONS vintages
│   └── processed/             # Tidy dataset output
├── requirements.txt
└── README.md
```

All detailed explanations and analysis are included in each py and ipynb file.

---

### 🔍 Key Insight  
When time series data undergo periodic revisions, aligning each observation with its corresponding **vintage date** enables us to:  
- Visualise the **revision dynamics** clearly,  
- Quantify how much later vintages adjust earlier estimates,  
- Use simple models effectively once the structure is consistent.  

In this dataset, the month-by-vintage grid turned out to be dense and stable, allowing even basic models (e.g. exponential smoothing or ARIMA) to achieve strong accuracy.

---

### 🧠 Highlights  
- Fully automated ingestion and cleaning pipeline.  
- Cross-vintage alignment for revision-tracking.  
- Lightweight forecasting that demonstrates the “less can be more” principle.  
- 100% reproducible on any system — no hard-coded paths.  

---

### ⚙️ Tech Stack  
- **Python 3** (pandas, requests, BeautifulSoup, dateutil)  
- **Matplotlib / Plotly** for visualisation  
- **Statsmodels / Scikit-learn** for forecasting experiments  

---

### 🚀 How to Run  

Run from the repository root:
1. **Fetch vintages**
   ```bash
   python src/fetch_vintages.py
   ```
2. **Build tidy dataset**
   ```bash
   python src/build_tidy.py
   ```
3. **Explore visualisations & forecasts**
   Open `analysis/visualisation.ipynb` and `analysis/forecast.ipynb` in JupyterLab or VS Code.

---

### 💬 Learning Outcome  
This project highlights that **data quality and structure often matter more than model complexity**.  
Once a dataset is properly aligned and cleaned, even simple models can deliver **highly reliable forecasts** — turning a routine dataset into a reproducible analytical product.

---

### Note
   `fetch_vintages.py` and `build_tidy.py` create the data/raw and data/processed folders automatically.
   Raw data are not included; they can be fetched directly from the ONS website using fetch_vintages.py.
   All scripts are path-agnostic and reproducible on any machine.
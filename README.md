# Amazon E-Commerce Data Engineering Pipeline  
*End-to-end data processing pipeline for Amazon sales analytics*

---

## 📌 Project Overview  
This project processes **multi-source Amazon sales data** (CSV files) into structured analytics-ready tables using a **medallion architecture** (Bronze → Silver → Gold). Designed for scalability with Delta Lake and cloud warehouses.

---

## 🛠️ Technical Architecture  
### Data Pipeline  
1. **Bronze Layer**: Raw CSV ingestion (`Amazon Sale Report.csv`, `International Sale Report.csv`, etc.)  
2. **Silver Layer**:  
   - Data cleaning & transformations (`silver_transformation.py`)  
   - Lookup tables generation (`lookup.py`, `lookup_silver_runtimedays.py`)  
3. **Gold Layer**:  
   - Analytics-ready tables (`gold_layer_delta_fietable.py`)  
   - Business metrics (P&L, warehouse comparisons)  

### Key Scripts  
| File | Purpose |  
|------|---------|  
| `autoloader.py` | Automated raw data ingestion |  
| `silver_stage.py` | Silver layer processing logic |  
| `debug_runtimedays_false.py` | Pipeline debugging tool |  

---

## 📂 Repository Structure  

---

## 🚀 Setup & Execution  
1. **Prerequisites**:  
   ```bash
   pip install pandas pyspark delta-spark
2. **Run Pipeline**:
   ```bash
   python autoloader.py                  # Ingestion  
   python silver_transformation.py       # Silver processing  
   python gold_layer_delta_fietable.py   # Gold layer

## 📊 Output Samples
1. **Silver Layer**: 
- Cleaned tables with enforced schemas.
2. **Gold Layer**


### Key Features:  
1. **Clear Medallion Architecture**: Highlights Bronze/Silver/Gold layers.  
2. **File Mapping**: Explains each script's purpose in a table.  
3. **Visual Cues**: Placeholder for warehouse comparison charts.  
4. **Reproducibility**: Simple setup commands.  

Adjust paths/visuals as needed based on your actual data!

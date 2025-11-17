# Project 2: Streaming Pipeline (Task 4)

This pipeline simulates real-time IoT sensor data using Kafka and PySpark, aggregating a count of readings per minute.

**Technology Stack:** Docker, Kafka, Zookeeper, PySpark (local), Python.

---

### 1. 🖥️ Environment Prerequisites (Mandatory)

This pipeline runs in a "hybrid" model (Broker in Docker, Processor in Local). Please ensure your **local machine** is set up correctly.

#### A. Java (JDK 17)
* PySpark requires a Java JDK to run.
* **Verify (in a new terminal):** `java -version` (Must show JDK 17).
* **Setup:** Ensure the `JAVA_HOME` System Variable is set correctly.
    * **`JAVA_HOME`** = `C:\...\[YOUR_PATH]\jdk-17.0.17.10-hotspot` (without `\bin`)

#### B. Hadoop `winutils` (Mandatory for PySpark on Windows)
PySpark on Windows requires `winutils.exe` to prevent `HADOOP_HOME` errors.
1.  Create `C:\hadoop\hadoop-3.3.6\bin`.
2.  Download [winutils.exe (v3.3.6)](https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.6/bin/winutils.exe) and [hadoop.dll (v3.3.6)](https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.6/bin/hadoop.dll).
3.  Place both files inside `C:\hadoop\hadoop-3.3.6\bin`.
4.  Set the following **System Variables**:
    * `HADOOP_HOME` = `C:\hadoop\hadoop-3.3.6` (without `\bin`)
    * Add this to your System `Path` = `%HADOOP_HOME%\bin`
5.  **RESTART your Terminal/VS Code** to load the new variables.
* **Verify (in a new terminal):** `winutils --help` (Must show the help text).

#### C. Python `venv`
1.  Create a virtual environment: `python -m venv venv`
2.  Activate it: `.\venv\Scripts\activate`
3.  Install libraries: `pip install -r requirements.txt`

---

### 2. 🚀 Run Instructions

**IMPORTANT:** Ensure the Batch pipeline (Airflow) is **not running** (`docker-compose down` in the other folder) to free up system RAM.

You will need **3 separate terminals** open in this project folder.

**A. Terminal 1: Start the Kafka Broker**
*(Ensures Docker Desktop is running)*
```bash
docker-compose -f docker-compose.streaming.yaml up -d
```
> _(Wait 1-2 minutes for Kafka to be ready)_

**B. Terminal 2: Run the Producer (Source)**_(Ensure venv is active)_

```Bash
python producer.py   `
```
> _Expected Output:** A continuous stream of INFO:__-main-__:Data diproduksi: .... logs. Leave this running_
    
**C. Terminal 3: Run the Consumer (Process & Sink)**_(Ensure venv is active and all prerequisites are met)_
```Bash
python consumer.py
```
> _Expected Output:** INFO:\_\_main\_\_:Koneksi Spark Session berhasil dibuat. followed by INFO:\_\_main\_\_:Menunggu agregasi...._
    
### 3\. 📊 Verification

Observe **Terminal 3 (Consumer)**. After 1-2 minutes, when the first 1-minute window completes, PySpark will print the aggregated results to the console, fulfilling the task requirement.
Expected Output:

![Streaming Pipeline Architecture](../assets/Output%20Task%204.png)


### 4\. 🛑 To Shut Down

1. Press Ctrl + C in Terminal 2 (Producer) and Terminal 3 (Consumer).
2. Run this in Terminal 1
```Bash
docker-compose -f docker-compose.streaming.yaml down
```

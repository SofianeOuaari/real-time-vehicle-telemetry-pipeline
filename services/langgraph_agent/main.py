"""
LangGraph Agent for Telemetry Analysis

AI-powered analysis using multi-agent collaboration with Ollama.
"""
import os
import json
import logging
from datetime import datetime
from pathlib import Path
import duckdb
from langchain_ollama import ChatOllama

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TelemetryAnalysisAgent:
    def __init__(self):
        self.ollama_url = os.getenv('OLLAMA_BASE_URL', 'http://ollama:11434')
        self.duckdb_path = os.getenv('DUCKDB_PATH', '/data/telemetry.duckdb')
        self.output_dir = Path(os.getenv('RESULTS_OUTPUT_PATH', '/data/analysis_results'))
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.llm = ChatOllama(base_url=self.ollama_url, model='llama3.2', temperature=0)
        logger.info("Telemetry analysis agent initialized")

    def load_data_summary(self):
        """Load summary statistics from DuckDB"""
        con = duckdb.connect(self.duckdb_path, read_only=True)

        # Get basic statistics
        stats = con.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT vehicle_id) as num_vehicles,
                AVG(speed_kmh) as avg_speed,
                AVG(engine_temp) as avg_temp,
                SUM(CASE WHEN overheating THEN 1 ELSE 0 END) as overheating_count
            FROM telemetry_data
        """).fetchone()

        # Get anomalies
        anomalies = con.execute("""
            SELECT vehicle_id, timestamp, engine_temp, speed_kmh
            FROM telemetry_data
            WHERE overheating OR low_oil_pressure OR battery_issue
            ORDER BY timestamp DESC
            LIMIT 10
        """).fetchall()

        con.close()

        return {
            'stats': {
                'total_records': stats[0],
                'num_vehicles': stats[1],
                'avg_speed': stats[2],
                'avg_temp': stats[3],
                'overheating_count': stats[4]
            },
            'anomalies': [{'vehicle': a[0], 'timestamp': str(a[1]), 'temp': a[2], 'speed': a[3]} for a in anomalies]
        }

    def analyze_anomalies(self, data_summary):
        """Use LLM to analyze anomalies"""
        prompt = f"""You are analyzing car sensor telemetry data. Here's the summary:

Total Records: {data_summary['stats']['total_records']}
Vehicles: {data_summary['stats']['num_vehicles']}
Average Speed: {data_summary['stats']['avg_speed']:.1f} km/h
Average Temperature: {data_summary['stats']['avg_temp']:.1f}°C
Overheating Events: {data_summary['stats']['overheating_count']}

Recent Anomalies:
{json.dumps(data_summary['anomalies'], indent=2)}

Analyze the data and provide:
1. A brief assessment of fleet health
2. Key anomalies detected
3. Potential root causes
4. Recommendations

Keep your response concise and actionable."""

        try:
            response = self.llm.invoke(prompt)
            return response.content
        except Exception as e:
            logger.error(f"Error calling LLM: {e}")
            return "Analysis unavailable due to LLM error"

    def generate_report(self, data_summary, analysis):
        """Generate structured report"""
        report = {
            'report_id': f"report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            'generated_at': datetime.utcnow().isoformat(),
            'summary': data_summary,
            'ai_analysis': analysis,
            'alerts': [
                {'severity': 'high', 'message': f"{data_summary['stats']['overheating_count']} overheating events detected"}
            ] if data_summary['stats']['overheating_count'] > 0 else []
        }

        # Save report
        output_file = self.output_dir / f"{report['report_id']}.json"
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)

        logger.info(f"Report saved to {output_file}")
        return report

    def run(self):
        """Main analysis workflow"""
        logger.info("Starting telemetry analysis...")

        # Load data
        data_summary = self.load_data_summary()
        logger.info(f"Loaded data: {data_summary['stats']['total_records']} records")

        # Analyze with LLM
        analysis = self.analyze_anomalies(data_summary)
        logger.info("AI analysis complete")

        # Generate report
        report = self.generate_report(data_summary, analysis)
        logger.info(f"Analysis complete. Report ID: {report['report_id']}")

        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    agent = TelemetryAnalysisAgent()
    agent.run()

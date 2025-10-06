"""
Script to generate batch data for the data pipeline.
"""
import pandas as pd
import numpy as np
import os
from datetime import datetime

def generate_sample_data(num_records=1000):
    """Generate sample data for the batch pipeline."""
    np.random.seed(42)
    timestamps = pd.date_range(start='2023-01-01', periods=num_records, freq='H')
    data = {
        'timestamp': timestamps,
        'value1': np.random.normal(0, 1, num_records),
        'value2': np.random.randint(1, 100, num_records),
        'category': np.random.choice(['A', 'B', 'C', 'D'], num_records)
    }
    return pd.DataFrame(data)

def save_batch_data(df, output_dir='data/batch_input'):
    """Save batch data to the specified directory."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f'batch_data_{timestamp}.parquet')
    df.to_parquet(output_path, index=False)
    print(f"Saved batch data to {output_path}")

if __name__ == "__main__":
    print("Generating batch data...")
    df = generate_sample_data()
    save_batch_data(df)
    print("Batch data generation completed.")

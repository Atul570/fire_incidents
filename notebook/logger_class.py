# Databricks notebook source
# COMMAND ----------
# Cell 1: Logging Setup (Reused from Bronze Layer)
from datetime import datetime
import time

class PipelineLogger:
    """Simple logging helper for Databricks notebooks"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.start_time = time.time()
        self.step_times = {}
        
    def _timestamp(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def info(self, message, **kwargs):
        """Log informational message"""
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        if context:
            print(f"ℹ️  [{self._timestamp()}] {message} | {context}")
        else:
            print(f"ℹ️  [{self._timestamp()}] {message}")
    
    def success(self, message, **kwargs):
        """Log success message"""
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        if context:
            print(f"✅ [{self._timestamp()}] {message} | {context}")
        else:
            print(f"✅ [{self._timestamp()}] {message}")
    
    def warning(self, message, **kwargs):
        """Log warning message"""
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        if context:
            print(f"⚠️  [{self._timestamp()}] WARNING: {message} | {context}")
        else:
            print(f"⚠️  [{self._timestamp()}] WARNING: {message}")
    
    def error(self, message, **kwargs):
        """Log error message"""
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        if context:
            print(f"❌ [{self._timestamp()}] ERROR: {message} | {context}")
        else:
            print(f"❌ [{self._timestamp()}] ERROR: {message}")
    
    def step_start(self, step_name):
        """Mark the start of a pipeline step"""
        self.step_times[step_name] = time.time()
        print(f"\n{'='*60}")
        print(f"🚀 [{self._timestamp()}] Starting: {step_name}")
        print(f"{'='*60}")
    
    def step_end(self, step_name):
        """Mark the end of a pipeline step"""
        if step_name in self.step_times:
            duration = time.time() - self.step_times[step_name]
            print(f"{'='*60}")
            print(f"✅ [{self._timestamp()}] Completed: {step_name} | Duration: {duration:.2f}s")
            print(f"{'='*60}\n")
    
    def pipeline_summary(self):
        """Print pipeline execution summary"""
        total_duration = time.time() - self.start_time
        print(f"\n{'='*60}")
        print(f"📊 PIPELINE SUMMARY: {self.pipeline_name}")
        print(f"{'='*60}")
        print(f"Total Duration: {total_duration:.2f}s")
        print(f"Completed at: {self._timestamp()}")
        print(f"{'='*60}\n")
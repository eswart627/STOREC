# STOREC — Distributed File System Pipeline Core

## Overview

STOREC is a research-oriented distributed storage prototype designed to evaluate **multi-pipeline erasure-coded write performance**. The system focuses on measuring how concurrent stripe processing improves throughput and resource utilization compared to traditional sequential pipelines.

This repository contains the **core pipeline engine**, which performs:

* File striping
* Reed–Solomon erasure coding
* Concurrent stripe execution
* Simulated distributed data transfer
* Performance metric collection

The implementation is intentionally minimal and modular to support controlled experiments rather than production deployment.


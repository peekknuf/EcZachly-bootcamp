### **Task Split Summary:**

- **Engineer 1:**  
  - Primary: Unit-level profit (Task A), Aggregate engagement (Task E)  
  - Secondary: Daily growth (Task D)  
  - **Total:** 3 tasks  

- **Engineer 2:**  
  - Primary: Aggregate profit (Task B)  
  - Secondary: Unit-level profit (Task A), Aggregate engagement (Task E)  
  - **Total:** 3 tasks  

- **Engineer 3:**  
  - Primary: Aggregate growth (Task C)  
  - Secondary: Aggregate profit (Task B)  
  - **Total:** 2 tasks  

- **Engineer 4:**  
  - Primary: Daily growth (Task D)  
  - Secondary: Aggregate growth (Task C)  
  - **Total:** 2 tasks  

---

### **On-Call Schedule Summary:**

- **Rotation:** Weekly or bi-weekly shifts (e.g., Engineer 1 → Week 1, Engineer 2 → Week 2, etc.).  
- **Holidays:** Distribute holiday on-call duties evenly, with compensatory time off for those covering holidays.  
- **Fairness:** Use a shared calendar or tool (e.g., PagerDuty) to track and balance on-call responsibilities over time.  

This ensures **fair task distribution** and a **balanced on-call schedule**, including holidays.


### Runbooks

#### 1. Pipeline Name: Profit  
**Types of data:**  
a. Revenue from accounts (investor-facing)  
b. Aggregate profit reported to investors  

**Owners:**  
- Primary: Engineer 2 (Aggregate profit - Task B)  
- Secondary: Engineer 1 (Unit-level profit - Task A)  

**Common Issues:**  
a. Numbers don’t align with numbers on accounts/filings - these numbers need to be verified by an accountant if so.  
b. Missing or incomplete revenue data from upstream systems.  
c. Delays in data aggregation due to system failures.  

**SLA’s:**  
a. Numbers will be reviewed once a month by the account team.  
b. Data must be updated and available by the 5th of each month.  

**Oncall Schedule:**  
a. Monitored by Engineer 2 (primary) and Engineer 1 (secondary), with weekly rotations.  
b. If something breaks, it needs to be fixed within 24 hours.  

---

#### 2. Pipeline Name: Growth  
**Types of data:**  
a. Aggregate growth metrics reported to investors (e.g., revenue growth, user growth)  

**Owners:**  
- Primary: Engineer 3 (Aggregate growth - Task C)  
- Secondary: Engineer 4 (Daily growth - Task D)  

**Common Issues:**  
a. Discrepancies in growth calculations due to data inconsistencies.  
b. Missing or delayed data from tracking systems.  
c. Incorrect aggregation due to duplicate or incomplete data.  

**SLA’s:**  
a. Monthly growth reports must be finalized by the 3rd of each month.  
b. Data must be available for investor reviews by the 5th of each month.  

**Oncall Schedule:**  
a. Monitored by Engineer 3 (primary) and Engineer 4 (secondary), with weekly rotations.  
b. Critical issues must be resolved within 12 hours.  

---

#### 3. Pipeline Name: Engagement  
**Types of data:**  
a. Aggregate engagement metrics reported to investors (e.g., retention rates, feature adoption)  

**Owners:**  
- Primary: Engineer 1 (Aggregate engagement - Task E)  
- Secondary: Engineer 2 (Aggregate engagement - Task E)  

**Common Issues:**  
a. Missing or incomplete event tracking data.  
b. Misalignment between engagement metrics and investor expectations.  
c. Delays in data processing due to high volume or system bottlenecks.  

**SLA’s:**  
a. Monthly engagement reports must be finalized by the 3rd of each month.  
b. Data must be available for investor reviews by the 5th of each month.  

**Oncall Schedule:**  
a. Monitored by Engineer 1 (primary) and Engineer 2 (secondary), with weekly rotations.  
b. Issues must be resolved within 24 hours.  

---

### General Notes:  
- **Investor-Focused Only:** These runbooks include only pipelines that directly report metrics to investors. Internal or experimental pipelines are excluded.  
- **Holiday Coverage:** On-call responsibilities during holidays should be rotated fairly, with compensatory time off for those covering.  
- **Documentation:** Ensure all runbooks are updated regularly and accessible to all stakeholders.  
- **Monitoring:** Use tools like PagerDuty or Datadog to automate alerts and track pipeline health.  

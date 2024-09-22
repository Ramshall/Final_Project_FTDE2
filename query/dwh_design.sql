CREATE TABLE IF NOT EXISTS warehouse."dim_employee" (
    "EmployeeID" VARCHAR(50) PRIMARY KEY,
    "Name" VARCHAR(100),
    "Gender" VARCHAR(10),
    "Age" INT,
    "Department" VARCHAR(50),
    "Position" VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS warehouse."fact_performance" (
    "PerformanceID" SERIAL PRIMARY KEY,
    "EmployeeID" VARCHAR(50),
    "Name" VARCHAR(100),
    "ReviewPeriod" VARCHAR(50),
    "Rating" NUMERIC(3, 1),
    "Comments" TEXT,
    FOREIGN KEY ("EmployeeID") REFERENCES warehouse."dim_employee"("EmployeeID")
);

CREATE TABLE IF NOT EXISTS warehouse."fact_payroll" (
    "PayrollID" SERIAL PRIMARY KEY,
    "EmployeeID" VARCHAR(50),
    "Name" VARCHAR(100),
    "Salary" NUMERIC,
    "OvertimePay" NUMERIC,
    "PaymentDate" DATE,
    FOREIGN KEY ("EmployeeID") REFERENCES warehouse."dim_employee"("EmployeeID")
);

CREATE TABLE IF NOT EXISTS warehouse."fact_training" (
    "TrainingID" SERIAL PRIMARY KEY,
    "EmployeeID" VARCHAR(50),
    "Name" VARCHAR(100),
    "TrainingProgram" VARCHAR(100),
    "StartDate" DATE,
    "EndDate" DATE,
    "Status" VARCHAR(20),
    FOREIGN KEY ("EmployeeID") REFERENCES warehouse."dim_employee"("EmployeeID")
);

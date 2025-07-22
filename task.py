from fastapi import FastAPI, HTTPException, Depends, status
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session

DATABASE_URL = "sqlite:///./workflows.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database Models
class Workflow(Base):
    __tablename__ = "workflows"
    id = Column(Integer, primary_key=True, index=True)
    workflow_str_id = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False)
    steps = relationship("Step", back_populates="workflow", cascade="all, delete-orphan")

class Step(Base):
    __tablename__ = "steps"
    id = Column(Integer, primary_key=True, index=True)
    step_str_id = Column(String, nullable=False)
    description = Column(String, nullable=True)
    workflow_id = Column(Integer, ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False)
    workflow = relationship("Workflow", back_populates="steps")
    prerequisites = relationship(
        "Dependency",
        foreign_keys="Dependency.step_id",
        back_populates="step",
        cascade="all, delete-orphan"
    )
    dependents = relationship(
        "Dependency",
        foreign_keys="Dependency.prerequisite_id",
        back_populates="prerequisite"
    )
    __table_args__ = (UniqueConstraint('step_str_id', 'workflow_id', name='_step_workflow_uc'),)

class Dependency(Base):
    __tablename__ = "dependencies"
    id = Column(Integer, primary_key=True, index=True)
    step_id = Column(Integer, ForeignKey("steps.id", ondelete="CASCADE"), nullable=False)
    prerequisite_id = Column(Integer, ForeignKey("steps.id", ondelete="CASCADE"), nullable=False)
    step = relationship("Step", foreign_keys=[step_id], back_populates="prerequisites")
    prerequisite = relationship("Step", foreign_keys=[prerequisite_id], back_populates="dependents")
    __table_args__ = (UniqueConstraint('step_id', 'prerequisite_id', name='_step_prereq_uc'),)

Base.metadata.create_all(bind=engine)

# Pydantic Schemas
class WorkflowCreate(BaseModel):
    workflow_str_id: str = Field(..., example="wf001")
    name: str = Field(..., example="Data Processing Pipeline")

class WorkflowResponse(BaseModel):
    internal_db_id: int
    workflow_str_id: str
    status: str

class StepCreate(BaseModel):
    step_str_id: str = Field(..., example="stepA")
    description: Optional[str] = Field(None, example="Download Data")

class StepResponse(BaseModel):
    internal_db_id: int
    step_str_id: str
    status: str

class DependencyCreate(BaseModel):
    step_str_id: str = Field(..., example="stepB")
    prerequisite_step_str_id: str = Field(..., example="stepA")

class StatusResponse(BaseModel):
    status: str

class StepDetail(BaseModel):
    step_str_id: str
    description: Optional[str]
    prerequisites: List[str]

class WorkflowDetail(BaseModel):
    workflow_str_id: str
    name: str
    steps: List[StepDetail]

class ExecutionOrder(BaseModel):
    order: List[str]

# Dependency
app = FastAPI(title="Workflow Definition API")

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 1. Create Workflow
@app.post("/workflows", response_model=WorkflowResponse)
def create_workflow(data: WorkflowCreate, db: Session = Depends(get_db)):
    existing = db.query(Workflow).filter(Workflow.workflow_str_id == data.workflow_str_id).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Workflow ID already exists")
    wf = Workflow(workflow_str_id=data.workflow_str_id, name=data.name)
    db.add(wf)
    db.commit()
    db.refresh(wf)
    return {"internal_db_id": wf.id, "workflow_str_id": wf.workflow_str_id, "status": "created"}

# 2. Add Step
@app.post("/workflows/{workflow_str_id}/steps", response_model=StepResponse)
def add_step(workflow_str_id: str, data: StepCreate, db: Session = Depends(get_db)):
    wf = db.query(Workflow).filter(Workflow.workflow_str_id == workflow_str_id).first()
    if not wf:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found")
    existing = db.query(Step).filter(Step.workflow_id == wf.id, Step.step_str_id == data.step_str_id).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Step ID already exists in this workflow")
    step = Step(step_str_id=data.step_str_id, description=data.description, workflow_id=wf.id)
    db.add(step)
    db.commit()
    db.refresh(step)
    return {"internal_db_id": step.id, "step_str_id": step.step_str_id, "status": "step_added"}

# 3. Add Dependency with validation
@app.post("/workflows/{workflow_str_id}/dependencies", response_model=StatusResponse)
def add_dependency(workflow_str_id: str, data: DependencyCreate, db: Session = Depends(get_db)):
    wf = db.query(Workflow).filter(Workflow.workflow_str_id == workflow_str_id).first()
    if not wf:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found")
    if data.step_str_id == data.prerequisite_step_str_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Self-dependency detected")
    step = db.query(Step).filter(Step.workflow_id == wf.id, Step.step_str_id == data.step_str_id).first()
    prereq = db.query(Step).filter(Step.workflow_id == wf.id, Step.step_str_id == data.prerequisite_step_str_id).first()
    if not step or not prereq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Step or prerequisite not found in workflow")
    # Prevent duplicate dependency
    existing = db.query(Dependency).filter(Dependency.step_id == step.id, Dependency.prerequisite_id == prereq.id).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Dependency already exists")
    dep = Dependency(step_id=step.id, prerequisite_id=prereq.id)
    db.add(dep)
    db.commit()
    return {"status": "dependency_added"}

# Milestone 1: Get details
@app.get("/workflows/{workflow_str_id}/details", response_model=WorkflowDetail)
def get_workflow_details(workflow_str_id: str, db: Session = Depends(get_db)):
    wf = db.query(Workflow).filter(Workflow.workflow_str_id == workflow_str_id).first()
    if not wf:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found")
    steps_out = []
    for step in wf.steps:
        prereqs = [dep.prerequisite.step_str_id for dep in step.prerequisites]
        steps_out.append(StepDetail(step_str_id=step.step_str_id, description=step.description, prerequisites=prereqs))
    return WorkflowDetail(workflow_str_id=wf.workflow_str_id, name=wf.name, steps=steps_out)

# Milestone 3: Execution order
@app.get("/workflows/{workflow_str_id}/execution-order", response_model=ExecutionOrder)
def get_execution_order(workflow_str_id: str, db: Session = Depends(get_db)):
    wf = db.query(Workflow).filter(Workflow.workflow_str_id == workflow_str_id).first()
    if not wf:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found")
    # Build graph
    steps = {step.step_str_id: step for step in wf.steps}
    in_degree: Dict[str, int] = {sid: 0 for sid in steps}
    graph: Dict[str, List[str]] = {sid: [] for sid in steps}
    for step in wf.steps:
        for dep in step.prerequisites:
            src = dep.prerequisite.step_str_id
            dst = dep.step.step_str_id
            graph[src].append(dst)
            in_degree[dst] += 1
    # Kahn's algorithm
    queue = [sid for sid, deg in in_degree.items() if deg == 0]
    order = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    if len(order) != len(steps):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="cycle_detected")
    return {"order": order}

# Root endpoint
@app.get("/")
def read_root():
    return {"message": "Workflow Definition API is running"}

# To run: uvicorn app:app --reload

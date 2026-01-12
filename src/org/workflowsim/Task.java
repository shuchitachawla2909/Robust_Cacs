/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package org.workflowsim;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.workflowsim.utils.Parameters.FileType;

/**
 * Task extends Cloudlet to support workflow execution.
 *
 * In addition to CloudSim's Cloudlet, this class:
 *  - maintains parent/child dependencies for DAG execution,
 *  - stores workflow input/output files,
 *  - exposes metadata used by offline and runtime schedulers.
 *
 * This class is also extended with CACS-specific fields
 * to support offline planning and priority-based runtime scheduling.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 */
public class Task extends Cloudlet {

    /* ================= Workflow structure ================= */

    /** Immediate parent tasks in the workflow DAG */
    private List<Task> parentList;

    /** Immediate child tasks in the workflow DAG */
    private List<Task> childList;

    /** Input and output data files associated with this task */
    private List<FileItem> fileList;

    /* ================= Scheduling metadata ================= */

    /** Generic priority (not used by default schedulers) */
    private int priority;

    /** Longest distance from workflow entry node (set during parsing) */
    private int depth;

    /** Research-related impact metric */
    private double impact;

    /** Optional task type label (workflow dependent) */
    private String type;

    /* ================= Execution bookkeeping ================= */

    /**
     * Actual finish time of the task.
     * Stored separately because Cloudlet does not expose a mutable finish time.
     */
    private double taskFinishTime;

    /** Original task length used to reinitialize Cloudlet if needed */
    private long runlength;

    /* ================= CACS-specific fields ================= */

    /**
     * CACS send start time (SVI):
     * Time at which this task begins sending data through the client I/O port
     * during offline planning.
     */
    private double cacsSvi;

    /**
     * CACS receive end time (EVI):
     * Time at which this task finishes receiving results through the client I/O port
     * during offline planning.
     */
    private double cacsEvi;

    /**
     * Final CACS priority rank.
     * Lower value means higher priority at runtime scheduling.
     */
    private int cacsRank = Integer.MAX_VALUE;

    /**
     * Constructs a workflow task.
     *
     * Cloudlet file sizes are not used directly; workflow data
     * transfer is modeled via {@link FileItem}.
     */
    public Task(final int taskId, final long taskLength) {

        super(taskId,
              taskLength,
              1,
              0,
              0,
              new UtilizationModelFull(),
              new UtilizationModelFull(),
              new UtilizationModelFull());

        this.runlength = taskLength;
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();

        this.impact = 0.0;
        this.taskFinishTime = -1.0;

        // Initialize CACS fields
        this.cacsSvi = 0.0;
        this.cacsEvi = 0.0;
    }

    /* ================= Basic metadata ================= */

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return this.priority;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getDepth() {
        return this.depth;
    }

    /* ================= DAG relationships ================= */

    public List<Task> getChildList() {
        return this.childList;
    }

    public void setChildList(List<Task> list) {
        this.childList = list;
    }

    public List<Task> getParentList() {
        return this.parentList;
    }

    public void setParentList(List<Task> list) {
        this.parentList = list;
    }

    public void addChild(Task task) {
        this.childList.add(task);
    }

    public void addParent(Task task) {
        this.parentList.add(task);
    }

    public void addChildList(List<Task> list) {
        this.childList.addAll(list);
    }

    public void addParentList(List<Task> list) {
        this.parentList.addAll(list);
    }

    /* ================= File handling ================= */

    public List<FileItem> getFileList() {
        return this.fileList;
    }

    public void setFileList(List<FileItem> list) {
        this.fileList = list;
    }

    public void addFile(FileItem file) {
        this.fileList.add(file);
    }

    public List<FileItem> getInputFileList() {
        List<FileItem> inputs = new ArrayList<>();
        for (FileItem file : fileList) {
            if (file.getType() == FileType.INPUT) {
                inputs.add(file);
            }
        }
        return inputs;
    }

    public List<FileItem> getOutputFileList() {
        List<FileItem> outputs = new ArrayList<>();
        for (FileItem file : fileList) {
            if (file.getType() == FileType.OUTPUT) {
                outputs.add(file);
            }
        }
        return outputs;
    }

    /* ================= CACS accessors ================= */

    public double getCacsSvi() {
        return this.cacsSvi;
    }

    public void setCacsSvi(double cacsSvi) {
        this.cacsSvi = cacsSvi;
    }

    public double getCacsEvi() {
        return this.cacsEvi;
    }

    public void setCacsEvi(double cacsEvi) {
        this.cacsEvi = cacsEvi;
    }

    public int getCacsRank() {
        return cacsRank;
    }

    public void setCacsRank(int rank) {
        this.cacsRank = rank;
    }

    /* ================= Execution tracking ================= */

    public void setTaskFinishTime(double time) {
        this.taskFinishTime = time;
    }

    public double getTaskFinishTime() {
        return this.taskFinishTime;
    }

    /**
     * Restores original Cloudlet length if reused.
     */
    public long initlength() {
        super.setCloudletLength(runlength);
        return runlength;
    }

    public void setImpact(double impact) {
        this.impact = impact;
    }

    public double getImpact() {
        return this.impact;
    }
}

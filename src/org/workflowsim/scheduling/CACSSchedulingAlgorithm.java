package org.workflowsim.scheduling;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowSimTags;

/**
 * Runtime scheduling component for the CACS strategy.
 *
 * This scheduler:
 *  - consumes task priorities produced by the offline CACS planner,
 *  - selects the highest-priority READY task,
 *  - assigns it to the VM that minimizes earliest finish time (EFT).
 *
 * Design notes:
 *  - No DAG traversal is performed here (dependencies are resolved offline).
 *  - No I/O-port or communication modeling is done at runtime.
 *  - This separation keeps runtime scheduling lightweight and efficient.
 */
public class CACSSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public CACSSchedulingAlgorithm() {
        super();
    }

    /**
     * Main scheduling loop.
     *
     * At each step:
     *  - pick the unscheduled task with the highest priority (lowest rank),
     *  - bind it to the currently IDLE VM with minimum EFT,
     *  - mark both task and VM as scheduled/busy.
     *
     * The behavior mirrors list-based schedulers such as HEFT,
     * but task ordering is dictated by the CACS offline planner.
     */
    @Override
    public void run() {

        int size = getCloudletList().size();

        // Attempt to schedule as many tasks as possible
        for (int i = 0; i < size; i++) {

            CondorVM bestVm = null;
            double bestEft = Double.MAX_VALUE;

            Cloudlet selected = null;

            // Select the highest-priority unscheduled task
            for (Object o : getCloudletList()) {
                Cloudlet cl = (Cloudlet) o;

                // Only workflow tasks are considered
                if (!(cl instanceof org.workflowsim.Task)) continue;
                if (getScheduledList().contains(cl)) continue;

                if (selected == null ||
                    cl.getClassType() < selected.getClassType()) {
                    selected = cl;
                }
            }

            // No remaining READY tasks
            if (selected == null) {
                break;
            }

            // Choose the IDLE VM that minimizes earliest finish time
            for (Object o : getVmList()) {
                CondorVM vm = (CondorVM) o;

                if (vm.getState() != WorkflowSimTags.VM_STATUS_IDLE) continue;

                double execTime =
                        selected.getCloudletTotalLength() / vm.getMips();

                double eft = CloudSim.clock() + execTime;

                if (eft < bestEft) {
                    bestEft = eft;
                    bestVm = vm;
                }
            }

            // No VM currently available (consistent with reference schedulers)
            if (bestVm == null) {
                break;
            }

            // Commit scheduling decision
            bestVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            selected.setVmId(bestVm.getId());
            getScheduledList().add(selected);
        }
    }
}

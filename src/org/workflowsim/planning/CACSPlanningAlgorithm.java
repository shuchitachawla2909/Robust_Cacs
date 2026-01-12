package org.workflowsim.planning;

import java.util.*;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.FileItem;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

/**
 * Offline planning component for the CACS scheduling strategy.
 *
 * This class:
 *  - performs a pre-simulation of workflow execution at the client side,
 *  - models a single I/O port by avoiding overlapping send/receive intervals,
 *  - assigns each task a priority based on its pre-simulated send start time.
 *
 * The produced priority is later consumed by the runtime scheduler.
 */
public class CACSPlanningAlgorithm extends BasePlanningAlgorithm {

    /** Final task ordering after offline planning */
    private List<Task> priorityList;

    /** Average bandwidth across all VMs (must be unit-consistent) */
    private double averageBandwidth;

    /** Average computation time of each task across eligible VMs */
    private Map<Task, Double> avgCompTime;

    /** Tracks occupied I/O intervals to avoid port collisions */
    private OverlapBuffer ob;

    /** Limit for factorial permutation when reordering parallel parents */
    private static final int MAX_PERMUTE_PARENTS = 6;

    /** Small epsilon to avoid floating-point overlap issues */
    private static final double EPS = 1e-9;

    public CACSPlanningAlgorithm() {
        priorityList = new ArrayList<>();
        avgCompTime = new HashMap<>();
        ob = new OverlapBuffer();
    }

    /**
     * Entry point for offline planning.
     *
     * Steps:
     *  - estimate computation and communication costs,
     *  - enumerate all root-to-leaf paths in the DAG,
     *  - schedule tasks in descending critical-path order,
     *  - assign a final priority to each task.
     */
    @Override
    public void run() {
        Log.printLine("CACS offline planner started");

        averageBandwidth = calculateAverageBandwidth();
        computeAvgCompTimes();

        // Enumerate all DAG paths and process longer paths first
        List<List<Task>> paths = enumerateAllPaths();
        paths.sort((a, b) -> Double.compare(pathTau(b), pathTau(a)));

        Set<Task> scheduled = new HashSet<>();

        // Schedule tasks path-by-path, ensuring dependencies are respected
        for (List<Task> path : paths) {
            for (Task t : path) {

                List<Task> unscheduledParents =
                        collectUnscheduledParents(t, scheduled);

                // If multiple parents are ready, try reordering them
                if (unscheduledParents.size() == 1) {
                    schedulePresim(unscheduledParents.get(0), scheduled);
                } else if (unscheduledParents.size() > 1) {
                    for (Task p : arrangeParallelParents(unscheduledParents)) {
                        schedulePresim(p, scheduled);
                    }
                }

                if (!scheduled.contains(t)) {
                    schedulePresim(t, scheduled);
                }
            }
        }

        // Safety net: schedule any remaining tasks
        for (Task t : getTaskList()) {
            if (!scheduled.contains(t)) {
                schedulePresim(t, scheduled);
            }
        }

        // Final priority is based on send start time (earlier = higher priority)
        priorityList = new ArrayList<>(getTaskList());
        priorityList.sort(Comparator
                .comparingDouble(Task::getCacsSvi)
                .thenComparingInt(Task::getCloudletId));

        int rank = 0;
        for (Task t : priorityList) {
            t.setCacsRank(rank++);
            t.setClassType(t.getCacsRank());
        }

        Log.printLine("CACS offline planning finished");
    }

    public List<Task> getPriorityList() {
        return priorityList;
    }

    /**
     * Pre-simulates sending, execution, and receiving of a task
     * while ensuring no two I/O operations overlap at the client.
     */
    private void schedulePresim(Task task, Set<Task> scheduled) {
        if (task == null || scheduled.contains(task)) return;

        // Ensure all parents are scheduled first
        List<Task> parents = task.getParentList();
        if (parents != null) {
            for (Task p : parents) {
                if (!scheduled.contains(p)) {
                    schedulePresim(p, scheduled);
                }
            }
        }

        double Lsend = computeLsend(task);
        double Lrecv = computeLrecv(task);
        double dvi = avgCompTime.getOrDefault(task, 1.0);

        // Earliest time after all parent results are available
        double ready = 0.0;
        if (parents != null) {
            for (Task p : parents) {
                ready = Math.max(ready, p.getCacsEvi() + 1.0);
            }
        }

        // Schedule send, execution, and receive sequentially
        double sendStart = ob.findEarliestGapAfter(ready, Lsend);
        double execEnd = sendStart + Lsend + dvi;
        double recvStart = ob.findEarliestGapAfter(execEnd, Lrecv);
        double recvEnd = recvStart + Lrecv;

        // Reserve I/O intervals to avoid collisions
        ob.insertInterval(sendStart - EPS, sendStart + Lsend + EPS);
        ob.insertInterval(recvStart - EPS, recvEnd + EPS);

        task.setCacsSvi(sendStart);
        task.setCacsEvi(recvEnd);

        scheduled.add(task);
    }

    /**
     * Attempts to reorder a set of parallel parent tasks to reduce
     * overall completion time under the single-port constraint.
     */
    private List<Task> arrangeParallelParents(List<Task> parents) {

        if (parents.size() <= MAX_PERMUTE_PARENTS) {

            List<List<Task>> perms = PermutationUtil.permute(parents);
            double best = Double.POSITIVE_INFINITY;
            List<Task> bestOrder = parents;

            for (List<Task> perm : perms) {
                OverlapBuffer tmpOb = ob.copy();
                Map<Task, Double> tmpEvi = new HashMap<>();

                for (Task t : perm) {
                    simulate(t, tmpOb, tmpEvi);
                }

                double makespan =
                        tmpEvi.values().stream()
                                .mapToDouble(v -> v)
                                .max()
                                .orElse(Double.POSITIVE_INFINITY);

                if (makespan < best) {
                    best = makespan;
                    bestOrder = perm;
                }
            }
            return bestOrder;
        }

        // Fallback heuristic for large parent sets
        parents.sort((a, b) -> Double.compare(taskTau(b), taskTau(a)));
        return parents;
    }

    /**
     * Lightweight simulation used during parent reordering.
     * Does not modify global state.
     */
    private void simulate(Task t,
                          OverlapBuffer tmpOb,
                          Map<Task, Double> tmpEvi) {

        double ready = 0.0;
        List<Task> parents = t.getParentList();
        if (parents != null) {
            for (Task p : parents) {
                double pe = tmpEvi.getOrDefault(p, p.getCacsEvi());
                ready = Math.max(ready, pe + 1.0);
            }
        }

        double Lsend = computeLsend(t);
        double Lrecv = computeLrecv(t);
        double dvi = avgCompTime.getOrDefault(t, 1.0);

        double sendStart = tmpOb.findEarliestGapAfter(ready, Lsend);
        double execEnd = sendStart + Lsend + dvi;
        double recvStart = tmpOb.findEarliestGapAfter(execEnd, Lrecv);
        double recvEnd = recvStart + Lrecv;

        tmpOb.insertInterval(sendStart - EPS, sendStart + Lsend + EPS);
        tmpOb.insertInterval(recvStart - EPS, recvEnd + EPS);

        tmpEvi.put(t, recvEnd);
    }

    /* ================= Graph utilities ================= */

    /** Enumerates all root-to-leaf paths in the workflow DAG */
    private List<List<Task>> enumerateAllPaths() {
        List<List<Task>> allPaths = new ArrayList<>();

        for (Task t : getTaskList()) {
            if (t.getParentList() == null || t.getParentList().isEmpty()) {
                dfs(t, new LinkedList<>(), allPaths);
            }
        }
        return allPaths;
    }

    private void dfs(Task current,
                     LinkedList<Task> currentPath,
                     List<List<Task>> allPaths) {

        currentPath.addLast(current);

        List<Task> children = current.getChildList();
        if (children == null || children.isEmpty()) {
            allPaths.add(new ArrayList<>(currentPath));
        } else {
            for (Task child : children) {
                if (!currentPath.contains(child)) {
                    dfs(child, currentPath, allPaths);
                }
            }
        }

        currentPath.removeLast();
    }

    /* ================= Cost helpers ================= */

    private double taskTau(Task t) {
        return computeLsend(t)
                + avgCompTime.getOrDefault(t, 1.0)
                + computeLrecv(t);
    }

    private double pathTau(List<Task> path) {
        return path.stream().mapToDouble(this::taskTau).sum();
    }

    private List<Task> collectUnscheduledParents(Task t,
                                                 Set<Task> scheduled) {
        List<Task> res = new ArrayList<>();
        List<Task> parents = t.getParentList();
        if (parents != null) {
            for (Task p : parents) {
                if (!scheduled.contains(p)) res.add(p);
            }
        }
        return res;
    }

    /** Computes average execution time of a task across all eligible VMs */
    private void computeAvgCompTimes() {
        for (Task t : getTaskList()) {
            double sum = 0;
            int c = 0;
            for (Object o : getVmList()) {
                CondorVM vm = (CondorVM) o;
                if (vm.getNumberOfPes() >= t.getNumberOfPes()) {
                    sum += t.getCloudletTotalLength() / vm.getMips();
                    c++;
                }
            }
            avgCompTime.put(t, c == 0 ? 1.0 : sum / c);
        }
    }

    private double calculateAverageBandwidth() {
        double sum = 0;
        for (Object o : getVmList()) {
            sum += ((CondorVM) o).getBw();
        }
        return Math.max(sum / getVmList().size(), 1.0);
    }

    /**
     * Estimates send time assuming:
     *  - File size in BYTES
     *  - Bandwidth in BITS / SECOND
     */
    private double computeLsend(Task t) {
        double bytes = 0;
        for (FileItem f : t.getFileList()) {
            if (f.getType() == Parameters.FileType.OUTPUT) {
                bytes += f.getSize();
            }
        }
        return (bytes * 8.0) / averageBandwidth;
    }

    private double computeLrecv(Task t) {
        return computeLsend(t);
    }

    /* ================= Support classes ================= */

    /**
     * Tracks occupied I/O intervals to prevent overlapping
     * send/receive operations at the client.
     */
    private static class OverlapBuffer {
        List<double[]> ints = new ArrayList<>();

        OverlapBuffer copy() {
            OverlapBuffer c = new OverlapBuffer();
            for (double[] i : ints) c.ints.add(i.clone());
            return c;
        }

        void insertInterval(double s, double e) {
            ints.add(new double[]{s, e});
            ints.sort(Comparator.comparingDouble(a -> a[0]));
        }

        double findEarliestGapAfter(double after, double len) {
            double t = after;
            for (double[] i : ints) {
                if (t + len <= i[0]) return t;
                t = Math.max(t, i[1]);
            }
            return t;
        }
    }

    /** Utility for bounded permutation generation */
    private static class PermutationUtil {
        static List<List<Task>> permute(List<Task> list) {
            List<List<Task>> res = new ArrayList<>();
            permute(list, 0, res);
            return res;
        }

        static void permute(List<Task> a, int i, List<List<Task>> r) {
            if (i == a.size()) {
                r.add(new ArrayList<>(a));
                return;
            }
            for (int j = i; j < a.size(); j++) {
                Collections.swap(a, i, j);
                permute(a, i + 1, r);
                Collections.swap(a, i, j);
            }
        }
    }
}

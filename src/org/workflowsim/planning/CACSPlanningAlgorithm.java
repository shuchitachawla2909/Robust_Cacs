package org.workflowsim.planning;

import java.util.*;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.FileItem;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

public class CACSPlanningAlgorithm extends BasePlanningAlgorithm {

    private List<Task> priorityList;
    private double averageBandwidth;
    private Map<Task, Double> avgCompTime;
    private OverlapBuffer ob;

    private static final int MAX_PERMUTE_PARENTS = 6;

    public CACSPlanningAlgorithm() {
        priorityList = new ArrayList<>();
        avgCompTime = new HashMap<>();
        ob = new OverlapBuffer();
    }

    @Override
    public void run() {
        Log.printLine("CACS offline planner started");

        averageBandwidth = calculateAverageBandwidth();
        computeAvgCompTimes();

        List<List<Task>> paths = enumerateAllPaths();
        paths.sort((a, b) -> Double.compare(pathTau(b), pathTau(a)));

        Set<Task> scheduled = new HashSet<>();

        for (List<Task> path : paths) {
            for (Task t : path) {
                List<Task> unscheduledParents = collectUnscheduledParents(t, scheduled);

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

        for (Task t : getTaskList()) {
            if (!scheduled.contains(t)) {
                schedulePresim(t, scheduled);
            }
        }

        priorityList = new ArrayList<>(getTaskList());
        priorityList.sort(Comparator
                .comparingDouble(Task::getCacsSvi)
                .thenComparingInt(Task::getCloudletId));
        
        System.out.println("========== CACS PRIORITY LIST ==========");
        System.out.println("TaskID\tSendStart\tFinish\tParents");

        for (Task t : priorityList) {
            String parents = (t.getParentList() == null || t.getParentList().isEmpty())
                    ? "None"
                    : t.getParentList().stream()
                        .map(p -> String.valueOf(p.getCloudletId()))
                        .reduce((a, b) -> a + "," + b)
                        .orElse("");

            System.out.println(
                    t.getCloudletId() + "\t" +
                    String.format("%.2f", t.getCacsSvi()) + "\t\t" +
                    String.format("%.2f", t.getCacsEvi()) + "\t\t" +
                    parents
            );
        }

        System.out.println("=======================================");
        
        Log.printLine("CACS offline planning finished");
    }

    public List<Task> getPriorityList() {
        return priorityList;
    }

    /* ================= Algorithm 2 ================= */

    private void schedulePresim(Task task, Set<Task> scheduled) {
        if (scheduled.contains(task)) return;

        for (Task p : task.getParentList()) {
            if (!scheduled.contains(p)) {
                schedulePresim(p, scheduled);
            }
        }

        double Lsend = computeLsend(task);
        double Lrecv = computeLrecv(task);
        double dvi = avgCompTime.get(task);

        double ready = 0.0;
        for (Task p : task.getParentList()) {
            ready = Math.max(ready, p.getCacsEvi() + 1.0);
        }

        double sendStart = ob.findEarliestGapAfter(ready, Lsend);
        double start = sendStart + Lsend;
        double finish = start + dvi;

        
        double recvStart = ob.findEarliestGapAfter(finish, Lrecv);
        double recvEnd = recvStart + Lrecv;

        
        ob.insertInterval(sendStart, sendStart + Lsend); // send
        ob.insertInterval(recvStart, recvEnd);          // receive

        task.setCacsSvi(sendStart);
        task.setCacsEvi(recvEnd);

        scheduled.add(task);
    }

    /* ================= Algorithm 3 ================= */

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

                double max = tmpEvi.values().stream().mapToDouble(v -> v).max().orElse(0);
                if (max < best) {
                    best = max;
                    bestOrder = perm;
                }
            }
            return bestOrder;
        }

        parents.sort((a, b) -> Double.compare(taskTau(b), taskTau(a)));
        return parents;
    }

    private void simulate(Task t, OverlapBuffer tmpOb, Map<Task, Double> tmpEvi) {
        double ready = 0.0;
        for (Task p : t.getParentList()) {
            ready = Math.max(ready, tmpEvi.getOrDefault(p, p.getCacsEvi()) + 1.0);
        }

        double Lsend = computeLsend(t);
        double Lrecv = computeLrecv(t);
        double dvi = avgCompTime.get(t);

        double sendStart = tmpOb.findEarliestGapAfter(ready, Lsend);
        double execStart = sendStart + Lsend;
        double execEnd = execStart + dvi;

        
        double recvStart = tmpOb.findEarliestGapAfter(execEnd, Lrecv);
        double recvEnd = recvStart + Lrecv;

        
        tmpOb.insertInterval(sendStart, sendStart + Lsend); // send
        tmpOb.insertInterval(recvStart, recvEnd);          // receive
        tmpEvi.put(t, recvEnd);
    }

    /* ================= Helpers ================= */
    
    private List<List<Task>> enumerateAllPaths() {
        List<List<Task>> allPaths = new ArrayList<>();

        // 1. find entry tasks
        List<Task> entryTasks = new ArrayList<>();
        for (Task t : getTaskList()) {
            if (t.getParentList() == null || t.getParentList().isEmpty()) {
                entryTasks.add(t);
            }
        }

        // 2. DFS from each entry
        for (Task entry : entryTasks) {
            LinkedList<Task> currentPath = new LinkedList<>();
            Set<Task> visited = new HashSet<>();
            dfs(entry, currentPath, visited, allPaths);
        }

        return allPaths;
    }
    
    private void dfs(Task current, LinkedList<Task> currentPath, Set<Task> visited, List<List<Task>> allPaths) {
		
		currentPath.addLast(current);
		visited.add(current);
		
		// exit task → store path
		if (current.getChildList() == null || current.getChildList().isEmpty()) {
		   allPaths.add(new ArrayList<>(currentPath));
		} else {
		   for (Task child : current.getChildList()) {
		       if (!visited.contains(child)) {
		           dfs(child, currentPath, visited, allPaths);
		       }
		   }
		}
		
		// backtrack
		currentPath.removeLast();
		visited.remove(current);
	}

    

    


    private double taskTau(Task t) {
        return computeLsend(t) + avgCompTime.get(t) + computeLrecv(t);
    }

    private double pathTau(List<Task> path) {
        return path.stream().mapToDouble(this::taskTau).sum();
    }

    private List<Task> collectUnscheduledParents(Task t, Set<Task> scheduled) {
        List<Task> res = new ArrayList<>();
        for (Task p : t.getParentList()) {
            if (!scheduled.contains(p)) res.add(p);
        }
        return res;
    }

    private void computeAvgCompTimes() {
        for (Task t : getTaskList()) {
            double sum = 0; int c = 0;
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
        for (Object o : getVmList()) sum += ((CondorVM) o).getBw();
        return Math.max(sum / getVmList().size(), 1.0);
    }

    private double computeLsend(Task t) {
        double bytes = 0;
        for (FileItem f : t.getFileList()) {
            if (f.getType() == Parameters.FileType.OUTPUT) bytes += f.getSize();
        }
        return (bytes / Consts.MILLION) * 8.0 / averageBandwidth;
    }

    private double computeLrecv(Task t) {
        return computeLsend(t);
    }

    /* ================= OB + Permutation ================= */

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

    private static class PermutationUtil {
        static List<List<Task>> permute(List<Task> list) {
            List<List<Task>> res = new ArrayList<>();
            permute(list, 0, res);
            return res;
        }

        static void permute(List<Task> a, int i, List<List<Task>> r) {
            if (i == a.size()) r.add(new ArrayList<>(a));
            for (int j = i; j < a.size(); j++) {
                Collections.swap(a, i, j);
                permute(a, i + 1, r);
                Collections.swap(a, i, j);
            }
        }
    }
}

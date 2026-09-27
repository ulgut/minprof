import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/** A live HotSpot workload for testing actual JVM heap dumps. */
public final class HeapWorkload {
    private static volatile byte[][] heapBlocks;
    private static volatile ByteBuffer[] directBlocks;
    private static volatile GraphNode[] graphNodes;
    private static volatile Object[] referenceArray;

    private static final int MIB = 1024 * 1024;
    private static final int PAGE = 4096;

    private static final class GraphNode {
        GraphNode next;
        GraphNode skip;
        long payload;
    }

    private static long number(String value, String option) {
        long parsed = Long.parseLong(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(option + " must be nonnegative");
        }
        return parsed;
    }

    private static byte[][] allocateHeap(long totalBytes, int blockBytes) {
        long count = (totalBytes + blockBytes - 1) / blockBytes;
        if (count > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("too many heap blocks");
        }
        byte[][] blocks = new byte[(int) count][];
        for (int i = 0; i < blocks.length; i++) {
            int size = (int) Math.min(blockBytes, totalBytes - (long) i * blockBytes);
            byte[] block = new byte[size];
            for (int offset = 0; offset < size; offset += PAGE) {
                block[offset] = (byte) (i + offset);
            }
            blocks[i] = block;
        }
        return blocks;
    }

    private static ByteBuffer[] allocateDirect(long totalBytes, int blockBytes) {
        long count = (totalBytes + blockBytes - 1) / blockBytes;
        if (count > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("too many direct blocks");
        }
        ByteBuffer[] blocks = new ByteBuffer[(int) count];
        for (int i = 0; i < blocks.length; i++) {
            int size = (int) Math.min(blockBytes, totalBytes - (long) i * blockBytes);
            ByteBuffer block = ByteBuffer.allocateDirect(size);
            for (int offset = 0; offset < size; offset += PAGE) {
                block.put(offset, (byte) (i + offset));
            }
            blocks[i] = block;
        }
        return blocks;
    }

    private static GraphNode[] allocateGraph(int count) {
        GraphNode[] nodes = new GraphNode[count];
        for (int i = 0; i < count; i++) {
            nodes[i] = new GraphNode();
            nodes[i].payload = i;
        }
        for (int i = 0; i < count; i++) {
            nodes[i].next = nodes[(i + 1) % count];
            nodes[i].skip = nodes[(int) ((i * 31L + 7) % count)];
        }
        return nodes;
    }

    public static void main(String[] args) throws Exception {
        long heapMiB = 0;
        long directMiB = 0;
        int nodes = 0;
        int arrayRefs = 0;
        String arrayMode = "shared";
        int blockMiB = 8;
        Path dump = null;
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("each option needs a value");
            }
            switch (args[i]) {
                case "--heap-mib" -> heapMiB = number(args[i + 1], args[i]);
                case "--direct-mib" -> directMiB = number(args[i + 1], args[i]);
                case "--nodes" -> nodes = Math.toIntExact(number(args[i + 1], args[i]));
                case "--array-refs" -> arrayRefs = Math.toIntExact(number(args[i + 1], args[i]));
                case "--array-mode" -> arrayMode = args[i + 1];
                case "--block-mib" -> blockMiB = Math.toIntExact(number(args[i + 1], args[i]));
                case "--dump" -> dump = Path.of(args[i + 1]);
                default -> throw new IllegalArgumentException("unknown option: " + args[i]);
            }
        }
        if (dump == null || blockMiB < 1 || blockMiB > 1024 ||
                !(arrayMode.equals("shared") || arrayMode.equals("alternating") || arrayMode.equals("unique")) ||
                (heapMiB == 0 && directMiB == 0 && nodes == 0 && arrayRefs == 0)) {
            throw new IllegalArgumentException("usage: HeapWorkload --heap-mib N --direct-mib N --nodes N --array-refs N --array-mode shared|alternating|unique --block-mib N --dump FILE");
        }

        int blockBytes = Math.multiplyExact(blockMiB, MIB);
        long start = System.nanoTime();
        heapBlocks = allocateHeap(Math.multiplyExact(heapMiB, MIB), blockBytes);
        directBlocks = allocateDirect(Math.multiplyExact(directMiB, MIB), blockBytes);
        graphNodes = allocateGraph(nodes);
        if (arrayRefs > 0) {
            Object[] refs = new Object[arrayRefs];
            Object first = new Object();
            Object second = arrayMode.equals("alternating") ? new Object() : first;
            for (int i = 0; i < refs.length; i++) {
                refs[i] = switch (arrayMode) {
                    case "shared" -> first;
                    case "alternating" -> (i & 1) == 0 ? first : second;
                    case "unique" -> new Object();
                    default -> throw new AssertionError(arrayMode);
                };
            }
            referenceArray = refs;
        }
        System.out.printf("allocated heap=%d MiB direct=%d MiB nodes=%d array_refs=%d array_mode=%s in %.3f s%n",
                heapMiB, directMiB, nodes, arrayRefs, arrayMode, (System.nanoTime() - start) / 1e9);

        Path parent = dump.toAbsolutePath().getParent();
        Files.createDirectories(parent);
        HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        if (bean == null) {
            throw new IllegalStateException("HotSpot heap dump MXBean is unavailable");
        }
        start = System.nanoTime();
        bean.dumpHeap(dump.toString(), true);
        System.out.printf("dump=%s bytes=%d seconds=%.3f%n", dump,
                Files.size(dump), (System.nanoTime() - start) / 1e9);
        // Keep the roots observable through the dump call.
        if (heapBlocks == null || directBlocks == null || graphNodes == null || (arrayRefs > 0 && referenceArray == null)) {
            throw new AssertionError("roots lost");
        }
    }
}

import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.*;
import java.text.NumberFormat;
import java.util.*;
import java.util.regex.*;

public class CSVSplitter {
    private final Path inputFile;
    private final String header;
    private final int partNumberWidth;
    private static final int BUFFER_SIZE = 8192 * 1024; // 8MB buffer
    private static final byte[] NEW_LINE = System.lineSeparator().getBytes();
    
    private static final Pattern SIZE_PATTERN = 
        Pattern.compile("^(\\d+)\\s*([KMGT]B?)?$", Pattern.CASE_INSENSITIVE);
    
    private static final Map<String, Long> UNIT_MULTIPLIERS = new HashMap<>();
    static {
        UNIT_MULTIPLIERS.put("K", 1024L);
        UNIT_MULTIPLIERS.put("KB", 1024L);
        UNIT_MULTIPLIERS.put("M", 1024L * 1024);
        UNIT_MULTIPLIERS.put("MB", 1024L * 1024);
        UNIT_MULTIPLIERS.put("G", 1024L * 1024 * 1024);
        UNIT_MULTIPLIERS.put("GB", 1024L * 1024 * 1024);
        UNIT_MULTIPLIERS.put("T", 1024L * 1024 * 1024 * 1024);
        UNIT_MULTIPLIERS.put("TB", 1024L * 1024 * 1024 * 1024);
    }

    public CSVSplitter(String inputPath) throws IOException {
        this.inputFile = Paths.get(inputPath);
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            this.header = reader.readLine();
            if (this.header == null) {
                throw new IOException("Empty CSV file");
            }
        }
        this.partNumberWidth = 3;
    }

    private static String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }


public void splitByLines(long linesPerPart) throws IOException {
    if (linesPerPart <= 0) {
        throw new IllegalArgumentException("Lines per part must be positive");
    }

    long fileSize = Files.size(inputFile);
    System.out.println("Input file size: " + formatSize(fileSize));
    System.out.println("Splitting into parts of " + linesPerPart + " lines each");

    byte[] headerBytes = (header + System.lineSeparator()).getBytes();
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    long totalBytesProcessed = 0;
    int partNumber = 1;
    long lastReportTime = System.currentTimeMillis();

    try (FileChannel inputChannel = FileChannel.open(inputFile, StandardOpenOption.READ)) {
        // Skip header in input file
        inputChannel.position(headerBytes.length);

        FileChannel outputChannel = createOutputChannel(partNumber++);
        writeHeader(outputChannel, headerBytes);
        
        long currentLineCount = 0;
        boolean inLine = false;
        
        while (inputChannel.read(readBuffer) != -1) {
            readBuffer.flip();
            
            while (readBuffer.hasRemaining()) {
                byte b = readBuffer.get();
                writeBuffer.put(b);
                
                // If write buffer is full, flush it
                if (!writeBuffer.hasRemaining()) {
                    writeBuffer.flip();
                    outputChannel.write(writeBuffer);
                    writeBuffer.clear();
                }
                
                if (b == '\n') {
                    currentLineCount++;
                    inLine = false;
                    
                    // Check if we need to start a new file
                    if (currentLineCount >= linesPerPart) {
                        // Flush any remaining data
                        if (writeBuffer.position() > 0) {
                            writeBuffer.flip();
                            outputChannel.write(writeBuffer);
                            writeBuffer.clear();
                        }
                        
                        // Close current file and start new one
                        outputChannel.close();
                        outputChannel = createOutputChannel(partNumber++);
                        writeHeader(outputChannel, headerBytes);
                        
                        currentLineCount = 0;
                    }
                } else {
                    inLine = true;
                }
            }
            
            // Update progress
            totalBytesProcessed += readBuffer.limit();
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastReportTime >= 1000) {
                System.out.printf("\rProcessing part %d, lines: %d, processed: %s (%.1f%%)", 
                    partNumber - 1,
                    currentLineCount,
                    formatSize(totalBytesProcessed),
                    (totalBytesProcessed * 100.0) / fileSize);
                lastReportTime = currentTime;
            }
            
            readBuffer.clear();
        }
        
        // Handle final line if it doesn't end with newline
        if (inLine) {
            currentLineCount++;
        }
        
        // Write any remaining data in the write buffer
        if (writeBuffer.position() > 0) {
            writeBuffer.flip();
            outputChannel.write(writeBuffer);
        }
        
        outputChannel.close();
    }
    
    System.out.printf("\nSplit completed: %d parts created, total size: %s\n",
        partNumber - 1, formatSize(totalBytesProcessed));
}

    private FileChannel createOutputChannel(int partNumber) throws IOException {
        Path outputPath = createPartPath(partNumber);
        return FileChannel.open(outputPath, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.WRITE, 
            StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void writeHeader(FileChannel channel, byte[] headerBytes) throws IOException {
        channel.write(ByteBuffer.wrap(headerBytes));
    }

    private Path createPartPath(int partNumber) {
        String fileName = inputFile.getFileName().toString();
        String baseName = fileName.substring(0, fileName.lastIndexOf('.'));
        String extension = fileName.substring(fileName.lastIndexOf('.'));
        String partFileName = String.format("%s_part%0" + partNumberWidth + "d%s",
            baseName, partNumber, extension);
        return inputFile.resolveSibling(partFileName);
    }

    public void splitBySize(long maxSizeBytes) throws IOException {
        if (maxSizeBytes <= 0) {
            throw new IllegalArgumentException("Max size must be positive");
        }

        long fileSize = Files.size(inputFile);
        System.out.println("Input file size: " + formatSize(fileSize));
        System.out.println("Splitting into parts of maximum " + formatSize(maxSizeBytes));

        // Pre-allocate direct buffers for better performance
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        byte[] headerBytes = (header + System.lineSeparator()).getBytes();

        try (FileChannel inputChannel = FileChannel.open(inputFile, StandardOpenOption.READ)) {
            // Skip header in input file
            long position = headerBytes.length;
            inputChannel.position(position);
            
            int partNumber = 1;
            long currentSize = 0;
            long totalBytesProcessed = 0;
            long lastReportTime = System.currentTimeMillis();
            FileChannel outputChannel = null;

            while (position < fileSize) {
                // Start new part if needed
                if (outputChannel == null || currentSize >= maxSizeBytes) {
                    if (outputChannel != null) {
                        writeBuffer.flip();
                        while (writeBuffer.hasRemaining()) {
                            outputChannel.write(writeBuffer);
                        }
                        outputChannel.close();
                    }
                    
                    // Create new part file
                    Path outputPath = createPartPath(partNumber++);
                    outputChannel = FileChannel.open(outputPath, 
                        StandardOpenOption.CREATE, 
                        StandardOpenOption.WRITE, 
                        StandardOpenOption.TRUNCATE_EXISTING);
                    
                    // Write header
                    writeBuffer.clear();
                    writeBuffer.put(headerBytes);
                    writeBuffer.flip();
                    outputChannel.write(writeBuffer);
                    writeBuffer.clear();
                    
                    currentSize = headerBytes.length;
                    
                    System.out.printf("\rCreated part %d, processed: %s (%.1f%%)", 
                        partNumber - 1, 
                        formatSize(totalBytesProcessed),
                        (totalBytesProcessed * 100.0) / fileSize);
                }

                // Read chunk
                readBuffer.clear();
                int bytesRead = inputChannel.read(readBuffer);
                if (bytesRead == -1) break;
                
                readBuffer.flip();
                
                // Process the chunk and write to output
                while (readBuffer.hasRemaining()) {
                    byte b = readBuffer.get();
                    writeBuffer.put(b);
                    currentSize++;
                    totalBytesProcessed++;
                    
                    // Flush write buffer if full
                    if (!writeBuffer.hasRemaining()) {
                        writeBuffer.flip();
                        outputChannel.write(writeBuffer);
                        writeBuffer.clear();
                    }
                }
                
                position += bytesRead;
                
                // Report progress every second
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastReportTime >= 1000) {
                    System.out.printf("\rProcessing part %d, total processed: %s (%.1f%%)", 
                        partNumber,
                        formatSize(totalBytesProcessed),
                        (totalBytesProcessed * 100.0) / fileSize);
                    lastReportTime = currentTime;
                }
            }

            // Clean up last part
            if (outputChannel != null) {
                writeBuffer.flip();
                while (writeBuffer.hasRemaining()) {
                    outputChannel.write(writeBuffer);
                }
                outputChannel.close();
            }

            System.out.printf("\nSplit completed: %d parts created, total size: %s\n",
                partNumber - 1, formatSize(totalBytesProcessed));
        }
    }
	
    public void splitByParts(int numberOfParts) throws IOException {
        if (numberOfParts <= 0) {
            throw new IllegalArgumentException("Number of parts must be positive");
        }

        long fileSize = Files.size(inputFile);
        long headerSize = (header + System.lineSeparator()).length();
        long dataSize = fileSize - headerSize;
        long sizePerPart = (dataSize + numberOfParts - 1) / numberOfParts;

        System.out.println("Total file size: " + formatSize(fileSize));
        System.out.println("Approximate size per part: " + formatSize(sizePerPart));
        
        splitBySize(sizePerPart);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java CSVSplitter <input-file> <split-type> <value>");
            System.out.println("Split types:");
            System.out.println("  -p <number>     Split into specified number of parts");
            System.out.println("  -l <number>     Split by number of lines per part");
            System.out.println("  -s <size>       Split by maximum size per part");
            System.out.println("\nSize format examples:");
            System.out.println("  1024   (bytes)");
            System.out.println("  10K    (kilobytes)");
            System.out.println("  100MB  (megabytes)");
            System.out.println("  1GB    (gigabytes)");
            System.out.println("  2TB    (terabytes)");
            System.exit(1);
        }

        try {
            CSVSplitter splitter = new CSVSplitter(args[0]);
            String splitType = args[1];
            String value = args[2];

            switch (splitType) {
                case "-p":
                    splitter.splitByParts(Integer.parseInt(value));
                    break;
                case "-l":
                    splitter.splitByLines(Long.parseLong(value));
                    break;
                case "-s":
                    splitter.splitBySize(parseSize(value));
                    break;
                default:
                    System.out.println("Invalid split type. Use -p, -l, or -s");
                    System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static long parseSize(String sizeStr) throws IllegalArgumentException {
        Matcher matcher = SIZE_PATTERN.matcher(sizeStr.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Invalid size format. Examples: 1024, 10K, 100MB, 1GB");
        }

        long size = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2);

        if (unit != null) {
            Long multiplier = UNIT_MULTIPLIERS.get(unit.toUpperCase());
            if (multiplier == null) {
                throw new IllegalArgumentException("Invalid size unit: " + unit);
            }
            size *= multiplier;
        }

        return size;
    }
}
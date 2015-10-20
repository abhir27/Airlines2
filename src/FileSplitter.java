import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileSplitter {
	public static long chunkSize = 10*1024*1024;
	public static void split(String filename) throws FileNotFoundException, IOException
	{
	BufferedInputStream in = new BufferedInputStream(new FileInputStream(filename));
File f = new File(filename);
	long fileSize = f.length();
	int subfile;
	for (subfile = 0; subfile < fileSize / chunkSize; subfile++)
		{BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(filename + "." + subfile));
				for (int currentByte = 0; currentByte < chunkSize; currentByte++)
			{
					out.write(in.read());
			}
		out.close();
		}
	if (fileSize != chunkSize * (subfile - 1))
		{
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(filename + "." + subfile));
			int b;
		while ((b = in.read()) != -1)
			out.write(b);
		
		out.close();			
		}
	in.close();
	}
}

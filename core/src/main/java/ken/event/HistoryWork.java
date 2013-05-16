package ken.event;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import ken.event.bus.Message;
import ken.event.util.FileUtil;
import ken.event.util.JDKSerializeUtil;

/**
 * Do recovery work after crash. It can be used by some other function modules.
 * 
 * @author xushuyan
 * 
 */
@SuppressWarnings("rawtypes")
public class HistoryWork implements Runnable {

	private String homeDir;
	private String allDir;
	private String hisDir;
	private String fileType;

	private LinkedBlockingQueue _waiting;
	private LinkedBlockingQueue _pending;

	public static Logger LOG = Logger.getLogger(HistoryWork.class);

	public HistoryWork() {

	}

	public HistoryWork(String dir, String adir, String hdir, String t,
			LinkedBlockingQueue w, LinkedBlockingQueue p) {

		homeDir = dir;
		allDir = adir;
		hisDir = hdir;
		fileType = t;
		_waiting = w;
		_pending = p;

	}

	@Override
	public void run() {

		String waitingDir = homeDir + File.separator + allDir;
		String forkDir = homeDir + File.separator + hisDir;

		try {
			moveFiles(waitingDir, forkDir);
			historyWork(forkDir);
		} catch (IOException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (ClassNotFoundException e) {
			LOG.error(e);
		} catch (Exception e) {
			LOG.error(e);
		}

	}

	private void moveFiles(String wdir, String fdir) throws IOException {

		LOG.info("moving history files from [" + wdir + "] to [" + fdir
				+ "]...");
		FileUtil.moveDirContent(wdir, fdir, fileType);

	}

	@SuppressWarnings("unchecked")
	private void historyWork(String fdir) throws IOException,
			ClassNotFoundException, Exception {

		int count = 0;
		File[] hisFiles;

		if (!FileUtil.isDirEmpty(fdir, fileType)) {
			hisFiles = FileUtil.listFiles(fdir, fileType);
			for (File hisFile : hisFiles) {

				// the way to read file into RAM depends on the file type
				if (fileType.equalsIgnoreCase(".msg")) {

					Message msg = new Message();
					FileUtil.readMsgFile(hisFile.getAbsolutePath(), msg);
					_waiting.put(msg);
					_pending.put(msg);
				} else if (fileType.equalsIgnoreCase(".evt")) {
					Event evt;

					byte[] evt_content = FileUtil.readFile(hisFile
							.getAbsolutePath());
					evt = (Event) JDKSerializeUtil.getObject(evt_content);
					_waiting.put(evt);
					_pending.put(evt);
				}

				FileUtil.deleteFile(hisFile.getAbsolutePath());
				count++;

			}
		}
		LOG.debug("read " + count
				+ "  history files and queue them for re_sending...  ");

	}

}

package ken.event;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import ken.event.bus.Message;
import ken.event.util.FileUtil;
import ken.event.util.JDKSerializeUtil;

@SuppressWarnings("rawtypes")
public class Swap implements Runnable {

	public static Logger LOG = Logger.getLogger(Swap.class);
	public final static int MODE_ALL = 0;
	public final static int MODE_DONE = 1;

	private int _mode;
	private String fileType;
	private String homeDir;
	private String DIR_ALL;
	private String DIR_DONE;
	private LinkedBlockingQueue _queue;
	private ConcurrentHashMap<String, String> _flag;

	public Swap(int mode, String type, String hdir, String ADIR, String DDIR,
			LinkedBlockingQueue queue, ConcurrentHashMap<String, String> flag) {

		_mode = mode;
		fileType = type;
		homeDir = hdir;
		DIR_ALL = ADIR;
		DIR_DONE = DDIR;
		_queue = queue;
		_flag = flag;

	}

	@Override
	public void run() {

		String dir;

		LOG.info("start the swap work...");
		try {
			switch (_mode) {
			case MODE_ALL:
				LOG.info("Now swap all data to folder...");
				dir = homeDir + File.separator + DIR_ALL;
				swapAll(dir);
			case MODE_DONE:
				LOG.info("Now swap processed data to folder...");
				dir = homeDir + File.separator + DIR_DONE;
				swapDone(dir);
			default:
				LOG.info("Invalid swap mode!");
				return;

			}
		} catch (IOException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		}

	}

	private void swapAll(String dir) throws IOException, InterruptedException {

		int count = 0;
		// String id ;

		while (!Thread.currentThread().isInterrupted()) {
			String id = null;

			if (fileType.equalsIgnoreCase(".msg")) {

				Message msg = (Message) _queue.take();
				
				List<byte[]> msg_item = new ArrayList<byte[]>();
				id = new String(msg.getId());
				msg_item.add(msg.getId());
				msg_item.add(msg.getPart1());
				msg_item.add(msg.getPart2());
				msg_item.add(msg.getPart3());
				FileUtil.writeLines(msg_item, dir, id + fileType);

			} else if (fileType.equalsIgnoreCase(".evt")) {

				Event evt = (Event) _queue.take();
				id = evt.getEventID().toString();
				byte[] event_content = JDKSerializeUtil.getBytes(evt);
				FileUtil.writeLine(event_content, dir, id + fileType);

			}

			if (_flag.putIfAbsent(id, "Y") != null) {
				LOG.warn("Seems like that id[" + id
						+ "] has already been logged!");
			}

			count++;

			LOG.debug("[" + count + "] files are swapped to in the directory  "
					+ dir);

		}

	}

	@SuppressWarnings("unchecked")
	private void swapDone(String dir) throws IOException, InterruptedException {
		int count = 0;
		while (!Thread.currentThread().isInterrupted()) {

			String id = (String) _queue.take();
			if (_flag.remove(id) != null) {

				FileUtil.moveFileToDir(homeDir + File.separator + DIR_ALL
						+ File.separator + id + fileType, dir);
				count++;
				LOG.debug("[" + count + "] files are swapped to directory " + dir);

				Thread.sleep(1000);
			} else {
				
				LOG.info("Not in the waiting folder, requeue it and try again later...");
				_queue.put(id);
				Thread.sleep(2000);
			}
			
		}

	}

}

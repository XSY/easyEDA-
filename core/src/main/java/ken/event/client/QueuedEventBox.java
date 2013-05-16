package ken.event.client;

import java.util.concurrent.LinkedBlockingQueue;

import ken.event.meta.AtomicE;

/**
 * @author KennyZJ
 * 
 */
public class QueuedEventBox extends LinkedBlockingQueue<AtomicE> implements
		EventBox {

	private static final long serialVersionUID = 842107820477539498L;

	public QueuedEventBox() {
		super();
	}

	public QueuedEventBox(int capacity) {
		super(capacity);
	}

	@Override
	public AtomicE take() throws InterruptedException {
		AtomicE evt;
		evt = super.take();
		return evt;
	}

	@Override
	public void put(AtomicE e) throws InterruptedException,
			NullPointerException {

		super.put(e);
	}

}

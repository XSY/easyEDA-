package ken.event;

@SuppressWarnings("rawtypes")
public class WaitingEvent extends Event {

	/**
	 * default serialVersionUID
	 */
	// private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	private static final long serialVersionUID = -5081260272071986482L;

	/**
	 * 10 second to wait
	 */
	public final static long DEFAULT_TIMEOUT = 10000;

	public final static long NEVER_TIMEOUT = 0;

	private long timeout;

	private long begin;

	public WaitingEvent() {
		this.begin = System.currentTimeMillis(); // only millisecond precise
		this.timeout = DEFAULT_TIMEOUT;
	}

	@SuppressWarnings("unchecked")
	public WaitingEvent(Event evt) {
		this();
		if (evt != null) {
			this.setEventID(evt.getEventID());
			this.setEvtType(evt.getEvtType());
			this.setAwaredTS(evt.getAwaredTS());
			this.setSource(evt.getSource());
			this.setLocation(evt.getLocation());
			this.setSecurityKey(evt.getSecurityKey());
			this.setEvtData(evt.getEvtData());
			this.setProducer(evt.getProducer());
		}
	}

	public WaitingEvent(long timeout) {
		this();
		if (timeout >= 0) {
			this.timeout = timeout;
		}
	}

	public WaitingEvent(Event evt, long timeout) {
		this(evt);
		if (timeout >= 0) {
			this.timeout = timeout;
		}
	}

	public long getTimeout() {
		return timeout;
	}

	public long getBegin() {
		return begin;
	}

	public boolean isExpired() {
		if (this.timeout == 0) {
			return false;
		}
		return (System.currentTimeMillis() - (begin + timeout)) >= 0;
	}
}

package iie.logmining.example;

public class Item {
	private String lfs;
	private String rfs;
	private double support;
	private double confidence;
	private double lift;
	
	public Item(String lfs,String rfs,double support,double confidence,double lift){
		this.lfs = lfs;
		this.rfs = rfs;
		this.support = support;
		this.confidence = confidence;
		this.lift = lift;
	}

	public String getLfs() {
		return lfs;
	}

	public void setLfs(String lfs) {
		this.lfs = lfs;
	}

	public String getRfs() {
		return rfs;
	}

	public void setRfs(String rfs) {
		this.rfs = rfs;
	}

	public double getSupport() {
		return support;
	}

	public void setSupport(double support) {
		this.support = support;
	}

	public double getConfidence() {
		return confidence;
	}

	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}

	public double getLift() {
		return lift;
	}

	public void setLift(double lift) {
		this.lift = lift;
	}

}
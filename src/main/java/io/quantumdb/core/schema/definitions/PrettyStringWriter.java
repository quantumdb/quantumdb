package io.quantumdb.core.schema.definitions;

class PrettyStringWriter implements Appendable, CharSequence {

	private final StringBuilder builder = new StringBuilder();

	private int indentation = 0;
	private boolean indent = true;

	public PrettyStringWriter modifyIndent(int delta) {
		this.indentation += delta;
		return this;
	}

	@Override
	public char charAt(int index) {
		return builder.charAt(index);
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		return builder.subSequence(start, end);
	}

	@Override
	public PrettyStringWriter append(CharSequence csq) {
		return append(csq, 0, csq.length());
	}

	@Override
	public PrettyStringWriter append(CharSequence csq, int start, int end) {
		for (int i = start; i < end; i++) {
			append(csq.charAt(i));
		}
		return this;
	}

	@Override
	public PrettyStringWriter append(char c) {
		if (indent) {
			indent = false;
			for (int i = 0; i < indentation; i++) {
				builder.append('\t');
			}
		}

		builder.append(c);

		if (c == '\n') {
			indent = true;
		}

		return this;
	}

	@Override
	public String toString() {
		return builder.toString();
	}

	@Override
	public int length() {
		return builder.length();
	}

}
